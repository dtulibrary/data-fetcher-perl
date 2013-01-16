package Data::Fetcher;

use strict;
use warnings;

use Data::Fetcher::File;
use Data::Fetcher::FileInfo;

our $VERSION = "1.1.0";

# public interface.

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self = {};

    $self = {@_};

    bless ($self, $class);

    eval {
        $self->init();
    };
    if ($@) {
        die join ('', __PACKAGE__, '::new() : ', $@);
    }

    return $self;
}


# Accessor methods (getters only) generated 
# from closures. The closure reference is 
# assigned to a typeglob of the appropriate name.

#for my $property_r (qw(
#)) {
#    my $slot   = $property_r;
#    my $method = $property_r;
#    $method =~ s/-/_/g;
#    no strict "refs";       # so symbolic ref to typeglob works.
#    *$method = sub {
#        my $self = shift;
#        return $self->{$slot};
#    }
#}


# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property_rw (qw(
    fi
    file-filter
    directory-filter
    remote-delete
    recursive
    directory
    separator
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

    $self->{separator} //= '/';
    $self->{recursive} //= 100;

    $self->fi(Data::Fetcher::FileInfo->new());

    return $self;
}

1;
__END__

=head1 NAME

Data::Fetcher - Basic data retrieval interface.

=head1 DESCRIPTION

Data fetcher base class.

Any sub class of Data::Fetcher should implement the 
list_files(), get_file() and delete_file() methods.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher constructor. 

  Returns a new Data::Fetcher subtype object.

  Throws exception on error.

=item init()

  Handles transport initialization, if any.

  Throws exception on error.

=item fi([$fi])

  Gets or sets file information handler as Data::Fetcher::FileInfo object.

=item file_filter([$filter])

  Gets or sets string to be used in a regular expression by the 
  list_files() method to add files to the list of retrievable
  files.

=item directory_filter([$filter])

  Gets or sets string to be used in a regular expression by the 
  list_files() method to select directories to traverse.

=item remote_delete([$remote_delete])

  Gets or sets remote deletion flag as boolean. If set to a true value
  remote files should be deleted after successful retrieval.
  Default is off.

=item recursive([$flag])
  
  Set the level of recursive handling while traversing.
  Unset value means unlimited traversel.

=item directory([$dir])

  Gets or sets directory from where files are fetched.
  Meaning differ between sub classes.

=item separator([$separator])

  Gets or sets path separator for remote file system (eg. / or \).
  Default is /;

=item list_files($dir, $recursive)

  Examines files found relative to specified directory $dir
  applying filter if any exists. If $dir is undefined the value of
  the 'dir' property is used instead.

  If $recursive is set to a true value any directories found under
  $dir will be recursively traversed. If $recursive is undefined 
  the value of the 'recursive' property is used instead.

  Returns retrievable files as array reference containing a 
  Data::Fetcher::File object with properties 'pi', 'size' and 'modified' 
  set accordingly for each file.

  Throws exception on error.

=item get_file($file_struct, $local_file)

  Stores file represented by a Data::Fetcher::File
  object $file_struct as local file specified by $local_file.

  Returns name of local file.

  Throws exception on error.

=item delete_file($file_struct)

  Deletes file represented by a Data::Fetcher::File object $file_struct.

  Returns path info of deleted file.

  Throws exception on error.

=back

=head1 AUTHOR

Jan Bauer Nielsen <jbn@dtic.dtu.dk>
Morten Rønne <mron@dtic.dtu.dk>
Michael Niedhardt <mine@dtic.dtu.dk>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
