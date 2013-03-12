package Data::Fetcher::File;

use strict;
use warnings;

our $VERSION = "1.1.0";

# public interface.

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self = {};

    $self = {@_};

    bless ($self, $class);
}


# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property_rw (qw(
    chksum
    modified
    pi
    size
    transfer_begin
    transfer_end
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

1;
__END__

=head1 NAME

Data::Fetcher::File - Data fetcher file representation.

=head1 SYNOPSIS

  use Data::Fetcher::File;

  my $file = new Data::Fetcher::File(
    'pi'       => $pi,       # filename + path
    'size'     => $size,     # file size in bytes
    'modified' => $last_mod, # last modification time 
                             # in seconds since the epoch.
  );

  $file->chksum($digest);

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::File constructor. 

  Return a new Data::Fetcher::File object.

=item chksum([$chksum])

  Gets or sets file checksum.

=item modified([$last_modified])

  Gets or sets last modification time of file
  as number of seconds since the epoch.

=item transfer_begin([$timestamp])

  Gets or sets timestamp marking the start of the actual file
  transfer process, be it ftp download, file system copy or
  OAI harvest, as number of seconds since the epoch.

=item transfer_end([$timestamp])

  Gets or sets timestamp marking the end of the actual file
  transfer process, as number of seconds since the epoch.

=item pi([$pi])

  Gets or sets path information of file.

=item size([$size])

  Gets or sets size of file in bytes.

=back

=head1 AUTHOR

Jan Bauer Nielsen, E<lt>jbn@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

