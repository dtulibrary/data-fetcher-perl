package Data::Fetcher::HTTP;

use strict;
use warnings;

use LWP;

use base qw(Data::Fetcher);

our $VERSION = "1.0.0";

# public interface.

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self;

    eval {
        $self = $class->SUPER::new('transport' => 'http', @_);
    };
    if ($@) {
        die join ('', __PACKAGE__, '::new() : ', $@);
    }

    return $self;
}

# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property_rw (qw(
    url
    local
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

sub init {
    my ($self) = @_;

    eval {
        $self = $self->SUPER::init;

        $self->{ua} = new LWP::UserAgent;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::init() : ', $@);
    }

    return $self;
}


sub list_files {
    my ($self) = @_;
    my $list = [];

    eval {
        my $fs = Data::Fetcher::File->new(
            'pi' => $self->local,
            'size' => 0,
            'modified' => time(),
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
    my ($response, $fethed, $out, $content, $fetched);

    eval {
        $fs->transfer_begin(time);

        $fetched = 0;
        $response = $self->{ua}->get($self->url());
        if($response && $response->is_success()) {
            $out = $self->fi->io_write($local_file, undef, ':encoding(UTF-8)');
            $content = $response->content();
            $content =~ s/\r//g;
            chomp($content);
            print $out $content,"\n";
            $out->close();

            $self->fi->test_archive($local_file)
                or die "$local_file archive validation test";

            $fs->chksum($self->fi->hexdigest($local_file));
            $fs->size($self->fi->size($local_file));

            # Use last modified from response if available
            $fs->modified($response->last_modified() || time());

        } else {
            die "HTTP error response ".
                $response->code()." ".$response->status_line;
        }

        $fs->transfer_end(time);
    };
    if ($@) {
        unlink $local_file;
        die join ('', __PACKAGE__, '::get_file() : ', $@);
    }

    return $local_file;
}

sub delete_file {
    my ($self, $fs) = @_;

    eval {
        die "HTTP DELETE not implemented";
    };
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}

1;
__END__

=head1 NAME

Data::Fetcher::HTTP - Data retrieval using HTTP protocol.

=head1 SYNOPSIS

  use Data::Fetcher::HTTP;

  my $fetcher = new Data::Fetcher::HTTP(
    url => $url,
    local => $file,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 DESCRIPTION

Data fetcher module using the HTTP protocol.
Sub class of the Data::Fetcher class.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::HTTP constructor.

  Returns a new Data::Fetcher::HTTP object.

  The constructor requires a url key/value pair to identify the remote
  file.

  Throws exception on error.

=item init

  Repository initialization. Retrieves repository granularity.

  Throws exception on error.

=item delete_file($fs)

  Stub method. Since a remote file as represented by Data::Fetcher::File
  object $fs normally isn't deleteable through http.

  Returns path info of "deleted" file.

  Throws exception on error.

=item get_file($fs, $local_file)

  Gets the file represented by the Data::Fetcher::File object $fs and stores
  it in the local file named by $local_file as a "plain" file.

  Returns name of written file, or empty string when no file where found.

  Throws exception on error.

=item list_files()

  Returns an array reference containing a file representation as a
  Data::Fetcher::File object.

  Throws exception on error.

=item url()

  Returns the URL to get.

=item local()

  Returns the filename to store in local filesystem.

=back

=head1 SEE ALSO

  Data::Fetcher man page.

=head1 AUTHOR

Morten Rønne, E<lt>mr@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

