package Data::Fetcher::FTPS;

use strict;
use warnings;

use File::Listing;
use Net::FTPSSL;

use base qw(Data::Fetcher);

our $VERSION = "1.1.0";

# private interface.

sub _list_files {
    my ($self, $list, $dir, $recursive) = @_;

    my $separator = $self->separator;
    $recursive //= $self->recursive;
    $dir //= $self->directory;

    my @listings = my @dir = ();

    @dir = $dir ? $self->ftps->list($dir) : $self->ftps->list();

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

        } elsif ($type eq 'f') {

            if (my $filter = $self->file_filter) {
                $filter = qr/$filter/i;
                next if ($name !~ m/$filter/);
            }

            $pi =~ s#^\./+##;

            my $fs = Data::Fetcher::File->new(
                'modified' => $mtime,
                'pi'       => $pi,
                'size'     => $size,
            );

            push (@$list, $fs);
        }
        elsif ($type =~ /^l/) {
            # What to do with symlinks?
        }
    }
}

# public interface.

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self;

    eval {
        $self = $class->SUPER::new('transport' => 'ftps', @_);
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
    ftps
    basename
    password
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

sub connect {
    my ($self) = @_;

    eval {
        $self->{ftps} = Net::FTPSSL->new(
            $self->basename,
            Port => 990, 
            Encryption => 'I', 
            Debug => 0)
        or die "FTPS connection failed: $@";

        $self->ftps->login($self->username, $self->password)
            or die "FTPS login failed: ", $self->ftps->message;

        $self->ftps->binary
            or die "Unable to set FTPS binary transfer mode";
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

        my $error_header = join (' ', 'ftps GET of file', $fs->pi, 'failed:');

        my $lsize = 0;
        my $rsize = $self->ftps->size($fs->pi);

        die "$error_header remote file has zero size" if ($rsize == 0);

        # Retrieve remote file.
        $self->ftps->get($fs->pi, $lfile)
            or die "$error_header ", $self->ftps->message;

        -e "$lfile"
            or die "$error_header local file $lfile missing";

        # Test if local file size matches size stored in file description
        # struct.
        $lsize = $self->fi->size($lfile);
        ($lsize == $fs->size)
            or die "$error_header local size $lsize differs from stored size ", 
                    $fs->size;

        ($lsize == $rsize) 
            or die "$error_header local size $lsize differ from remote size $rsize";

        $self->fi->test_archive($lfile)
            or die "$error_header archive validation test";

        $fs->chksum($self->fi->hexdigest($lfile));

        $fs->transfer_end(time);
    };
    if ($@) {
        my $err = $@;
        $self->{ftps} = undef;
        $self->connect();
        die join ('', __PACKAGE__, '::get_file() : ', $err);
    }

    return $lfile;
}


sub delete_file {
    my ($self, $fs) = @_;

    eval {
        my $error_header = join (' ', 'ftps DEL of file', $fs->pi, 'failed:');

        $self->ftps->delete($fs->pi)
            or die $error_header, ' ', $self->ftps->message;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::delete_file() : ', $@);
    }

    return $fs->pi;
}


1;
__END__

=head1 NAME

Data::Fetcher::FTPS - Data retrieval using FTPS (i.e. over SSL) protocol.
This was originally introduced because of ASTM requirements.

=head1 SYNOPSIS

  use Data::Fetcher::FTPS;

  my $fetcher = new Data::Fetcher::FTPS(
    'basename'       => $host,
    'username'       => $user,
    'password'       => $pass,
    'file-filter'    => $filter,
    'remote-delete'  => $rdelete,
    'directory'      => $dir,
  );

  foreach (@{$fetcher->list_files()}) {
    $fetcher->get_file($_, $local_file);
  }

=head1 DESCRIPTION

Data fetcher module using the FTPS (i.e. over SSL) protocol.

Sub class of the Data::Fetcher class.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::FTPS constructor. 

  Returns a new Data::Fetcher::FTPS object.

  Throws exception on error.

=item connect

  Connect to the FTP server

=item init
  Set up connetion info needed.

=item ftps([$ftps])

  Gets or sets the FTPS handler as a Net::FTPSSL object.

=item basename([$host])

  Gets or sets the FTPS server host.

=item password([$pass])

  Gets or sets the password for the FTPS account.

=item username([$user])

  Gets or sets the username for the FTPS account.

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

Michael Neidhardt, E<lt>mine@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
