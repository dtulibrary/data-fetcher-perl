package Data::Fetcher::OAI;

use strict;
use warnings;

use DateTime;
use DateTime::Format::ISO8601;
use HTTP::OAI;
use XML::LibXML;
use Data::Dumper;

use base qw(Data::Fetcher);

our $VERSION = "1.1.0";

# public interface.

# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property_rw (qw(
    format
    set
    from
    until
    latest-datestamp
    earliest-datestamp
)) {
    my $slot   = $property_rw;
    my $method = $property_rw;
    $method =~ s/-/_/g;
    no strict "refs";       # so symbolic ref to typeglob works.
    *$method = sub {
        my $self = shift;
        $self->{$slot} = shift if @_;
        return $self->{$slot};
    }
}

# Accessor methods (getters only) generated 
# from closures. The closure reference is 
# assigned to a typeglob of the appropriate name.

for my $property_r (qw(
    basename
    oai
)) {
    my $slot   = $property_r;
    my $method = $property_r;
    $method =~ s/-/_/g;
    no strict "refs";       # so symbolic ref to typeglob works.
    *$method = sub {
        my $self = shift;
        return $self->{$slot};
    }
}

sub init {
    my ($self) = @_;
    
    eval {
        $self = $self->SUPER::init;

        $self->{oai} = 
            HTTP::OAI::Harvester->new(
                baseURL => $self->basename,
            );

        $self->timeout($self->{timeout});

        $self->format('oai_dc')
            unless ($self->format);

        my $response = $self->oai->Identify;
        if ($response->is_error) {
            die "error identifying repository ",$self->basename, " (",
                $response->code, ") ", $response->message;
        }

        $self->{datestamp_format} = 
            $self->{granularity} = $response->granularity;

        $self->{datestamp_format} =~ s/Y+/%Y/;
        $self->{datestamp_format} =~ s/MM/%m/;
        $self->{datestamp_format} =~ s/D+/%d/;
        $self->{datestamp_format} =~ s/h+/%H/;
        $self->{datestamp_format} =~ s/mm/%M/;
        $self->{datestamp_format} =~ s/s+/%S/;

        $self->earliest_datestamp(
            new DateTime::Format::ISO8601->parse_datetime(
                $response->earliestDatestamp
            )
        );
        $self->earliest_datestamp()->set_time_zone('UTC');

        unless (defined $self->from) {
            $self->from($self->earliest_datestamp);
        }
        unless (defined $self->until) {
            $self->until(
                $self->add_delta_granularity(
                    new DateTime::Format::ISO8601->parse_datetime(
                        $response->responseDate()
                    ), -1
                )
            );
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::init() : ', $@);
    }

    return $self;
}

sub delta_month {
    my ($until, $from) = @_;

    return $until->delta_md($from)->delta_months() > 1;
}


sub delta_year {
    my ($until, $from) = @_;

    return $until->delta_md($from)->delta_months() > 12;
}

sub list_files {
    my ($self) = @_;
    my ($period, $fs, $no, $until, $func, $from);
    my $list = [];

    eval {
        # Figure out how many files we should be getting
        if ($self->{set_limit}) {
            $self->from()->set_time_zone('UTC');
            $self->until()->set_time_zone('UTC');
            $period = $self->{set_limit};
            $func = $self->can("delta_$period");
            if ($func && $func->($self->until(), $self->from())) {
                # We need to split into multiple downloads
                $no = 1;
                $from = $self->from();
                while($from < $self->{until}) {
                    $until = $self->calculate_limit_fetch($from, $period);
                    $fs = Data::Fetcher::File->new(
                        'pi' => join ('', time, '_', $no, '.', $self->format, '.xml'),
                        'modified' => time(),
                        'oai_from' => $from,
                        'oai_until' => $until,
                        'size' => 0
                    );
                    push (@$list, $fs);
                    $from = $self->add_delta_granularity($until, 1);
                    # Adjust for leap second
                    $from = $self->add_delta_granularity($from, 1)
                        if($from->second() == 60);
                    $no++;
                }
                return;
            }
        }

        # We only need a single download
        $fs = Data::Fetcher::File->new(
            'pi' => join ('', time, '.', $self->format, '.xml'),
            'modified' => time(),
            'size' => 0
        );
        push (@$list, $fs);

    };
    if ($@) {
        die join ('', __PACKAGE__, '::list_files() : ', $@);
    }

    return $list;
}

sub get_file {
    my ($self, $fs, $local_file) = @_;

    eval {
        $fs->transfer_begin(time);

        my ($from, $until);
        # Upgrade epoch values to full DateTime objects.
        if($self->{from} && ref($self->from) ne 'DateTime') {
            $self->from(new DateTime->from_epoch($self->from));
        }
        if($self->{until} && ref($self->until) ne 'DateTime') {
            $self->until(new DateTime->from_epoch($self->until));
        }
        if ($fs->{oai_from}) {
            # The file object limits the period.
            $from = $fs->{oai_from}->strftime($self->{datestamp_format});
            $until = $fs->{oai_until}->strftime($self->{datestamp_format});
        } else {
            # Use global from/until
            if($self->from) {
                $self->from->set_time_zone('UTC');
                if($self->earliest_datestamp() &&
                   $self->from() lt $self->earliest_datestamp()) {
                    $self->from($self->earliest_datestamp);
                }
                $from = $self->from->strftime($self->{datestamp_format});
            }
            if($self->until) {
                $self->until->set_time_zone('UTC');
                $until = $self->until->strftime($self->{datestamp_format});
                $until = $from if ($from && ($until lt $from));
            }
        }

        my $rs = $self->oai->ListRecords(
            metadataPrefix => $self->format,
            from => $from,
            until => $until,
            set =>  $self->set,
        );
        die $rs->message if ($rs->is_error);

        my $out = $self->fi->io_write($local_file, undef, ':encoding(UTF-8)');

        my $header = q{};
        if ($rs->version eq '2.0') {
            $header =
                '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" '.
                         'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'. 
                         'xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/'. 
                            'http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"'.
                '>';
        } else {
            die 'unknown OAI protocol version ', $self->version;
        }

        my (undef, $query_string) = split (/\?/, $rs->requestURL, 2);
        my @attributes;
        for (split /\&/, $query_string) {
            my ($key,$val) = split /=/;
            for ($key, $val) {
                tr/+/ /;
                s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
            }
            push (@attributes, join ('', $key, '="', xml_escape($val), '"'));
        }
        $header = join ("\n", $header,
            '<responseDate>'.$rs->responseDate.'</responseDate>',
            '<request '.join (' ', @attributes).'>'.$self->basename.'</request>',
            '<ListRecords>',
        );

        print $out $header, "\n";
        
        local $XML::LibXML::skipXMLDeclaration = 1;
        my $latest_datestamp = q{};
        my $rec_count = 0;
        while (my $rec = $rs->next) {
            print $out '<record>', "\n";
            print $out $rec->header->dom->toString, "\n";
            # Metadata may not be present if it is a delete
            print $out $rec->metadata->dom->toString, "\n"
                if($rec->metadata);
            print $out '</record>', "\n";

            $latest_datestamp = $rec->header->datestamp
                if ($rec->header->datestamp gt $latest_datestamp);

            $rec_count++;
        }
        die $rs->message if ($rs->is_error);
        
        print $out '</ListRecords>', "\n";
        print $out '</OAI-PMH>', "\n";

        $self->fi->test_archive($local_file)
            or die "$local_file archive validation test";
        
        $fs->chksum($self->fi->hexdigest($local_file));
        $fs->size($self->fi->size($local_file));

        if($latest_datestamp) {
            $self->latest_datestamp(
                new DateTime::Format::ISO8601->parse_datetime($latest_datestamp)
            );
            $fs->modified($self->latest_datestamp->epoch);
        }

        $fs->transfer_end(time);

        unless ($rec_count) {
            $fs->size(0);
            unlink $local_file;
            $local_file = q{};
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::get_file() : ', $@);
    }

    return $local_file;

}

sub delete_file {
    my ($self, $fs) = @_;

    eval {};
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}

sub timeout {
    my ($self, $seconds) = @_;

    if (defined ($seconds) && ($seconds >= 0)) {
        $self->oai->timeout($seconds);
    }

    return $self->oai->timeout;
}


sub add_delta_granularity {
    my ($self, $date, $delta) = @_;

    # Get a copy of the date that we can change
    $date = $date->clone;
    $date->set_time_zone('UTC');
    $delta ||= 1;
    if ($self->{granularity} =~ m/hh:mm:ss/) {
        $date->add(seconds => $delta);
    } else {
        $date->add(days => $delta);
    }
    return $date;
}

   
sub calculate_limit_fetch {
    my ($self, $from, $period) = @_;
    my ($until);

    if ($period eq 'month') {
        $until = DateTime->last_day_of_month(
            year => $from->year(),
            month => $from->month(),
            hour  => 23,
            minute  => 59,
            second  => 59)->set_time_zone('UTC');
    } elsif ($period eq 'year') {
        $until = DateTime->new(
            year => $from->year(),
            month => 12,
            day => 31,
            hour  => 23,
            minute  => 59,
            second  => 59)->set_time_zone('UTC');
    } else {
        die "Unknown OAI fetch limit: ".$period;
    }
    die "Caught by leap second" if ($until < $from);
    if ($until > $self->{until}) {
        $until = $self->{until};
    }
    return $until;
}


sub xml_escape {
    my ($string, $all) = @_;

    $string =~ s/&/&amp;/g;
    $string =~ s/</&lt;/g;
    if ($all) {
        $string =~ s/>/&gt;/g;
        $string =~ s/'/&apos;/g;
        $string =~ s/"/&quot;/g;
    }
    return $string;
}

1;
__END__

=head1 NAME

Data::Fetcher::OAI - Data retrieval using OAI-PMH protocol.

=head1 SYNOPSIS

  use Data::Fetcher::OAI;

  my $fetcher = new Data::Fetcher::OAI(
    basename => $url,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 DESCRIPTION

Data fetcher module using the OAI-PMH protocol.

Sub class of the Data::Fetcher class.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::OAI constructor. 

  Returns a new Data::Fetcher::OAI object.

  The constructor requires a basename key/value pair to identify the remote
  OAI repository.

  An optional timeout key/value pair can be set, specifying in seconds the
  HTTP client timeout. A value of 0 disables timeout entirely, so use
  with caution.

  Throws exception on error.

=item init

  Repository initialization. Retrieves repository granularity. 

  Throws exception on error.

=item delete_file($fs)

  Stub method. Since a remote file as represented by Data::Fetcher::File
  object $fs doesn't exist it can't be deleted.

  Returns path info of "deleted" file.

  Throws exception on error.
  
=item get_file($fs, $local_file)

  Gets the full record set represented by the Data::Fetcher::File 
  object $fs and stores it in the local file named by $local_file as a OAI-PMH
  ListRecords reponse.

  As a side effect the 'modified' property of the Data::Fetcher::File
  object is set to the unix timestamp value of the latest datastamp seen in the
  record set.

  Returns name of written file, or empty string when no records where fetched.

  Throws exception on error.
  
=item list_files()

  Returns an array reference containing a record set representation as a 
  Data::Fetcher::File object.

  Throws exception on error.

=item oai()

  Returns the OAI harvester as a HTTP::OAI::Harvester object.

=item basename()

  Returns the OAI repository base URL as string.

=item format([$metadataPrefix])

  Gets or sets the format that should be included in the metadata part of the
  records in the OAI response.

  Default is oai_dc.

=item set([$set])

  Gets or sets a set criteria as string to be used for selective harvesting.

=item from([$timestamp])

  Gets or sets timestamp value to be used as a lower bound for datestamp-based
  selective harvesting.
  This may either be a unix timestamp (seconds since 1970) or a DateTime
  object.
  During get_file a unix timestamp will be converted to a DateTime object.

=item until([$timestamp])

  Gets or sets timestamp value to be used as an upper bound for datestamp-based
  selective harvesting.
  This may either be a unix timestamp (seconds since 1970) or a DateTime
  object.
  During get_file a unix timestamp will be converted to a DateTime object.

  Default value is set to the datestamp of the initial Identify response.

=item timeout([$seconds])

  Gets or sets the HTTP timeout interval in seconds.

  Default is 180. A value of 0 disables timeouts.

=item latest_datestamp($timestamp)

  Gets or sets the latest timestamp seen in the response from the
  repository.
  The value set/returned must be a DateTime object.

=item earliest_datestamp($timestamp);

  Gets or sets the earliest timestamp that the repository has.
  This value should come from the Identify operation.
  The value set/returned must be a DateTime object.


=item add_delta_granularity($date, $delta)

  Adds a delta (duration) to a date with the granularity of the repository.
  That means add days if repository accepts dates (granularity = YYYY-MM-DD),
  otherwise (granularity = YYYY-MM-DDThh:mm:ssZ) add seconds.

  $date must be a DateTime object.

  The returned date will be a copy and in the UTC timezone.

=item calculate_limit_fetch($from, $period)

  Calculate the until part of a fetch based on $from and $period.
  $from must be a DateTime object.
  $period must be the string "md" or "year" to tell wheter the period
  should be a month or a year.
  Will return a DataTime object for the end of the period.

=item delta_month($from, $until)

  Return true if dates are more than 1 month apart

=item delta_year($from, $until)

  Return true if dates are more than 12 month apart

=item xml_escape($value, $all)

  Convert value to xml encoding.
  Will always convert:
    &   ->  &amp;
    <   ->  &lt;
  if $all is true will these also be converted
    >   ->  &gt;
    '   ->  &qout;
    "   ->  &apos;

  true must be used for attribute values.

=back

=head1 SEE ALSO

  Data::Fetcher man page.

=head1 AUTHOR

Jan Bauer Nielsen, E<lt>jbn@dtic.dtu.dkE<gt>
Morten Rønne, E<lt>mr@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
