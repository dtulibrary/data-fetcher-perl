package Data::Fetcher::SFTP;

use strict;
use warnings;

use File::Listing;
use Net::SFTP::Foreign;

use base qw(Data::Fetcher);

our $VERSION = "1.0.0";

# private interface.

sub _list_files {
    my ($self, $list, $dir, $recursive) = @_;

    my $separator = $self->separator;
    $recursive //= $self->recursive;
    $dir //= $self->directory;

    my @listings = my $info = [];

    $info = $dir ? $self->sftp->ls($dir) : $self->sftp->ls();
    
    # preparse hack to get file listing parsed
    foreach (@$info) {
        $_ = $_->{longname};
        s/^.{10}TCP/----------/;    # strange filemod format like -AR--M----TCP
        s/^-\s+/---------- /;       # NETWARE format
        push (@listings, $_);
    }

    foreach (sort (parse_dir(\@listings))) {
        my ($name, $type, $size, $mtime, $mode) = @{$_};

        my $pi = $name;
        if ($dir) {
            $separator = '\\'
                if ($dir =~ m#\\#);

            unless ($name =~ m/^$dir/) {
                $pi = join ($separator, $dir, $name);
            }
        }

        if ($type eq 'd') {
            if ($recursive) {
                if (my $filter = $self->directory_filter) {
                    $filter = qr/$filter/i;
                    next if ($name !~ m/$filter/);
                }
                $self->_list_files($list, $pi, $recursive - 1);
            }

        } elsif ($type eq 'f') {
            $pi =~ s#^\./+##;

            next if ($self->no_hidden && $pi =~ m#^\.#);

            if (my $filter = $self->file_filter) {
                $filter = qr/$filter/i;
                next if ($name !~ m/$filter/);
            }

            my $fs = Data::Fetcher::File->new(
                'modified' => $mtime,
                'pi'       => $pi,
                'size'     => $size,
            );

            push (@$list, $fs);
        }
    }
}

# public interface.

# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property_rw (qw(
    sftp
    basename
    no_hidden
    password
    sshcmd
    timeout
    username
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

        $self->{sshcmd} //= 'ssh';
        $self->{timeout} //= 10;
        $self->{no_hidden} //= 1; 
        
        $self->{sftp} = 
            Net::SFTP::Foreign->new(
                host => $self->basename(),
                user => $self->username(), 
                timeout => $self->timeout(),
                password => $self->password(), 
                ssh_cmd => $self->sshcmd()
            );

        !$self->sftp->error
            or die "SSH connection failed: ", $self->sftp->error;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::init() : ', $@);
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

        my $error_header = join (' ', 'ftp GET of file', $fs->pi, 'failed:');

        my $lsize = 0;
        my $rsize = $self->sftp->stat($fs->pi)->size;

        # Test if remote file has any content.
        $rsize
             or die "$error_header remote file has zero size";

        # Retrieve remote file.
        $self->sftp->get($fs->pi, $lfile)
            or die "$error_header ", $self->sftp->error;

        -e "$lfile"
            or die "$error_header local file $lfile missing";

        # Test if local file size matches size stored in file description
        # struct.
        $lsize = $self->fi->size($lfile);
        ($lsize == $fs->size)
            or die "$error_header local size $lsize differs from stored size ",
                    $fs->size;

        # Test if remote file size matches local size.
        $rsize = $self->sftp->stat($fs->pi)->size;
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
        my $error_header = join (' ', 'sftp deletion of file', $fs->pi, 'failed:');

        $self->sftp->remove($fs->pi)
            or die $error_header, ' ', $self->sftp->error;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}


1;
__END__

=head1 NAME

Data::Fetcher::SFTP - Data retrieval using secure file transfer protocol.

=head1 SYNOPSIS

  use Data::Fetcher::SFTP;

  my $fetcher = new Data::Fetcher::SFTP(
    'basename'        => $host,
    'username'        => $user,
    'password'        => $pass,
    'file-filter'     => $filter,
    'remote-delete'   => $rdelete,
    'directory'       => $dir,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 DESCRIPTION

Data fetcher module using the secure file transfer protocol.

Sub class of the Data::Fetcher class.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::SFTP constructor. 

  Returns a new Data::Fetcher::SFTP object.

  Throws exception on error.

=item init

  Set up the connection info needed.

=item sftp([$sftp])

  Gets or sets the SFTP handler as a Net::SFTP::Foreign object.

=item basename([$host])

  Gets or sets the remote server hostname.

=item password([$pass])

  Gets or sets the remote user password.

=item username([$user])

  Gets or sets the username to log into the remote server.

=item no_hidden([$bool])

  Gets or sets the no_hidden flag (0/1). If true hidden files are
  not considered by the list_files() method. Default valus is 1.
  Note: this mothod assumes LINUX/UNIX style of representing
  hidden files.

=item sshcmd([$cmd])

  Gets or sets the name of the external SSH client. Default is
  'ssh'.

=item timeout([$timeout])

  Gets or sets the timeout in seconds before the connection
  is dropped if no data arrives on the SSH socket. Default 
  is 10 seconds.

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

=item list_files ($dir, $recursive)

  Examines files found relative to specified remote directory $dir
  applying filter if any exists. If $dir is undefined the value of
  the 'dir' property is used instead.

  If $recursive is set to a true value any directories found under
  $dir will be recursively traversed. If $recursive is undefined 
  the value of the 'recursive' property is used instead.

  Returns retrievable files as array reference containing a 
  Data::Fetcher::File object with properties 'pi', 'size' and 'modified' 
  set accordingly for each remote file.

  Throws exception on error.

=back

=head1 SEE ALSO

  Data::Fetcher man page.

=head1 AUTHOR

Jan Bauer Nielsen, E<lt>jbn@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
