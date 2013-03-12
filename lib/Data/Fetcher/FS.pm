package Data::Fetcher::FS;

use strict;
use warnings;

use File::Copy;
use Data::Fetcher::File;

use base qw(Data::Fetcher);

our $VERSION = "1.1.0";

# private interface.

sub _list_files {
    my ($self, $list, $dir, $recursive) = @_;

    my $separator = $self->separator;
    $recursive //= $self->recursive;
    $dir //= $self->directory;

    #if ($dir && $dir ne '.' && $dir ne '..') {
    if ($dir) {
        $separator = '\\'
            if ($dir =~ m#\\#);

        $dir =~ s#/\.$##;
        $dir =~ s#\/+$##;
        #$dir =~ s#\\+$##;

        my $dirhandle;
        opendir ($dirhandle, $dir)
            or die "unable to open $dir for reading";

        my @listings = grep {$_ ne '.' && $_ ne '..'} readdir $dirhandle;

        foreach (sort @listings) {
            my $pi = join ($separator, $dir, $_);
            my @stats = stat $pi;

            if (-d $pi) {
                if ($recursive) {
                    if (my $filter = $self->directory_filter) {
                        $filter = qr/$filter/i;
                        next if ($_ !~ m/$filter/);
                    }
                    $self->_list_files($list, $pi, $recursive);
                }

            } elsif (-f $pi) {
                if (my $filter = $self->file_filter) {
                    $filter = qr/$filter/i;
                    next if ($_ !~ m/$filter/);
                }

                $pi =~ s#^\./+##;
                my $file_struct = Data::Fetcher::File->new(
                    'modified' => $stats[9],
                    'pi'       => $pi,
                    'size'     => $stats[7],
                );

                push (@$list, $file_struct);
            }
        }

        closedir $dirhandle;
    }
}


# public interface.

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self;

    eval {
        $self = $class->SUPER::new('transport' => 'fs', @_);
    };
    if ($@) {
        die join ('', __PACKAGE__, '::new() : ', $@);
    }
    
    return $self;
}


sub list_files {
    my ($self, $dir, $recursive) = @_;
    my $list = [];

    eval {
        $self->_list_files($list, $dir, $recursive);
    };
    if ($@) {
        die join ('', __PACKAGE__, '::list_files() : ', $@);
    }

    return $list;
}


sub get_file {
    my ($self, $fs, $lfile) = @_;

    eval {
        $fs->transfer_begin(time);

        my $error_header = join (' ', 'retrieval of file', $fs->pi, 'failed:');

        my $rsize = my $lsize = 0;

        $rsize = $self->fi->size($fs->pi);

        # Test if remote file has any content.
        $rsize
             or die "$error_header remote file has zero size";

        # Retrieve remote file.
        copy ($fs->pi, $lfile);

        -e "$lfile"
            or die "$error_header local file $lfile missing";

        # Test if local file size matches remote size stored in 
        # file description struct.
        $lsize = $self->fi->size($lfile);
        ($lsize == $fs->size)
            or die "$error_header local size $lsize differs from stored size ", 
                    $fs->size;

        # Test if remote file size matches local size.
        ($lsize == $rsize)
            or die "$error_header local size $lsize differs from remote size ",
                   $rsize;

        $self->fi->test_archive($lfile)
            or die "$error_header archive validation test";

        $fs->chksum($self->fi->hexdigest($lfile));

        $fs->transfer_end(time);
    };
    if ($@) {
        die join ('', __PACKAGE__, '::get_file() : ', $@);
    }

    return $lfile;
}


sub delete_file {
    my ($self, $fs) = @_;

    eval {
        unlink ($fs->pi)
            or die 'deletion of file ', $fs->pi, ' failed';
    };
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}

1;
__END__

=head1 NAME

Data::Fetcher::FS - Data retrieval from local file system.

=head1 SYNOPSIS

  use Data::Fetcher::FS;

  my $fetcher = new Data::Fetcher::FS(
    'filter'  => $filter,
    'rdelete' => $rdelete,
    'rdir'    => $rdir,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::FS constructor. 
  
  Returns a new Data::Fetcher::FS object.

  Throws exception on error.

=item delete_file ($fs)

  Deletes remote file represented by a Data::Fetcher::File
  object $fs.

  Returns path info of deleted file.

  Throws exception on error.

=item get_file ($fs, $local_file)

  Stores remote file represented by a Data::Fetcher::File
  object $fs as local file specified by $local_file.

  Returns name of local file.

  Throws exception on error.

=item list_files ($rdir, $recursive)

  Examines files found relative to specified remote directory $rdir
  applying filter if any exists. If $rdir is undefined the value of
  the 'rdir' property is used instead.

  If $recursive is set to a true value any directories found under
  $rdir will be recursively traversed. If $recursive is undefined 
  the value of the 'recursive' property is used instead.

  Returns retrievable files as array reference containing a 
  Data::Fetcher::File object with properties 'pi', 'size' and 'modified' 
  set accordingly for each remote file.

  Throws exception on error.

=item directory

  Names the directory in the local file system where files should be fetched
  from.
  Filters are applied to the search.

  See Data::Fetcher for more about filters.

=back

=head1 SEE ALSO

  Data::Fetcher man page.

=head1 AUTHOR

Jan Bauer Nielsen, <jbn@dtic.dtu.dk>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
