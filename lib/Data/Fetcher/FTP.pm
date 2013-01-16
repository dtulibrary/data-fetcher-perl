package Data::Fetcher::FTP;

use strict;
use warnings;

use File::Listing;
use Net::FTP;

use base qw(Data::Fetcher);

our $VERSION = "1.0.0";

# private interface.

sub _list_files {
    my ($self, $list, $dir, $recursive) = @_;

    my $separator = $self->separator;
    $recursive //= $self->recursive;
    $dir //= $self->directory;

    my @listings = my @dir = ();

    @dir = $dir ? $self->ftp->dir($dir) : $self->ftp->dir();

    # preparse hack to get filelisting parsed
    foreach (@dir) {
        s/^.{10}TCP/----------/;    # strange filemod format like -AR--M----TCP
        s/^-\s+/---------- /;       # NETWARE format
        push (@listings, $_);
    }

    foreach (parse_dir(\@listings)) {
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

        } elsif ($type eq 'f' || $type =~ /^l /) {
            if (my $filter = $self->file_filter) {
                $filter = qr/$filter/i;
                next if ($name !~ m/$filter/);
            }

            # If a symlink then we would like the true size, not the size
            # of the symlink
            # We just have to pray that the FTP server supports size
            if($type =~ /^l /) {
                my $rsize = $self->ftp->size($pi);
                $size = $rsize if($rsize);
            }
            $pi =~ s#^\./+##;
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

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self;

    eval {
        $self = $class->SUPER::new('transport' => 'ftp', @_);
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
    ftp
    basename
    password
    passive
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

# Accessor methods (getters only) generated 
# from closures. The closure reference is 
# assigned to a typeglob of the appropriate name.

for my $property_r (qw(
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


sub connect {
    my ($self) = @_;

    eval {
        $self->{ftp} = Net::FTP->new(
            $self->basename,
            Debug => 0, 
            Passive => $self->passive,
        );

        $self->ftp
            or die "ftp connection failed: $@";

        $self->ftp->login($self->username, $self->password)
            or die "ftp login failed: ", $self->ftp->message;

        $self->ftp->binary
            or die "unable to set ftp binary transfer mode";
    };
    if ($@) {
        die join ('', __PACKAGE__, '::connect() : ', $@);
    }
}


sub init {
    my ($self) = @_;
    
    eval {
        $self = $self->SUPER::init;

        $self->{passive} //= 0;

        $self->connect();
    };
    if ($@) {
        die join ('', __PACKAGE__, '::init() : ', $@);
    }

    return $self;
}


sub is_file {
    my ($self, $rfile) = @_;
    my $is_file = 0;

    eval {
        if ($self->exists($rfile) && !$self->is_dir($rfile)) {
            $is_file = 1;
        } 
    };
    if ($@) {
        die join ('', __PACKAGE__, '::is_file() : ', $@);
    }
   
    return $is_file;
}


sub is_dir {
    my ($self, $dir) = @_;
    my $is_dir = 0;

    eval {
        my $c = $self->ftp->pwd();
        my $r = $self->ftp->cwd($dir);
        my $d = $self->ftp->cwd($c); 
        my $e = $self->ftp->pwd();

        if ($c ne $e || !$d) {
            die "unable to CWD into original directory $c";
        }

        if ($r) {
            $is_dir = 1;
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::is_dir() : ', $@);
    }
   
    return $is_dir;
}


sub exists {
    my ($self, $r) = @_;
    my $exists = 0;

    eval {
        if (defined $self->ftp->size($r) || $self->is_dir($r)) { 
            $exists = 1;
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::exists() : ', $@);
    }

    return $exists;
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

        my $rsize = my $lsize = 0;

        # We need to handle special cases were we are unable to query
        # the size of the remote file.
        $rsize = $self->ftp->size($fs->pi);
        unless (defined $rsize) {
            $rsize = $fs->size;
        }

        # Test if remote file has any content.
        $rsize
             or die "$error_header remote file has zero size";

        # Retrieve remote file.
        $self->ftp->get($fs->pi, $lfile)
            or die "$error_header ", $self->ftp->message;

        -e "$lfile"
            or die "$error_header local file $lfile missing";


        # Test if local file size matches size stored in file description
        # struct.
        $lsize = $self->fi->size($lfile);
        ($lsize == $fs->size)
            or die "$error_header local size $lsize differs from stored size ", 
                    $fs->size;

        # We need to handle special cases were we are unable to query
        # the size of the remote file.
        $rsize = $self->ftp->size($fs->pi);
        unless (defined $rsize) {
            $rsize = $fs->size;
        }

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
        my $err = $@;
        $self->{ftp} = undef;
        $self->connect();
        die join ('', __PACKAGE__, '::get_file() : ', $err);
    }

    return $lfile;
}


sub delete_file {
    my ($self, $fs) = @_;

    eval {
        my $error_header = join (' ', 'ftp DEL of file', $fs->pi, 'failed:');

        $self->ftp->delete($fs->pi)
            or die $error_header, ' ', $self->ftp->message;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}


1;
__END__

=head1 NAME

Data::Fetcher::FTP - Data retrieval using FTP protocol.

=head1 SYNOPSIS

  use Data::Fetcher::FTP;

  my $fetcher = new Data::Fetcher::FTP(
    'basename'      => $host,
    'username'      => $user,
    'password'      => $pass,
    'filter'        => $filter,
    'remote-delete' => $rdelete,
    'directory'     => $dir,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 DESCRIPTION

Data fetcher module using the FTP protocol.

Sub class of the Data::Fetcher class.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::FTP constructor. 

  Returns a new Data::Fetcher::FTP object.

  Throws exception on error.

=item init

  Set up connetion info needed.

=item connect

  Connect to the FTP server

=item ftp([$ftp])

  Gets or sets the FTP handler as a Net::FTP object.

=item basename([$host])

  Gets or sets the FTP server host.

=item password([$pass])

  Gets or sets the password for the FTP account.

=item passive([$passive])

  Gets or sets passive mode flag as boolean for the FTP transfer.
  Default is off.

=item username([$user])

  Gets or sets the username for the FTP account.

=item is_file($rfile)

  Returns true if a file specified by string $rfile exists
  on remote FTP server, else false.

  This method is guaranteed to work correctly across all FTP
  servers.

  Throws exception on error.

=item is_dir($dir)

  Returns true if a directory specified by string $dir exists
  on remote FTP server, else false.

  This method is guaranteed to work correctly across all FTP
  servers.

  Throws exception on error.

=item exists($r)

  Returns true if an entry specified by string $r exists
  on remote FTP server either as a file or a directory, 
  else false.

  This method is guaranteed to work correctly across all FTP
  servers.

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
