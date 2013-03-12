package Data::Fetcher::FileInfo;

use strict;
use warnings;

use Errno qw/EINTR/;
use IO::File;
use IO::Pipe;

our $VERSION = "1.1.0";

# private interface.

use constant F => 0;   # character never appears in text
use constant T => 1;   # character appears in plain ASCII text
use constant I => 2;   # character appears in ISO-8859 text
use constant X => 3;   # character appears in non-ISO extended ASCII 

my @text_chars = (
    #                  BEL BS HT LF    FF CR    
    F, F, F, F, F, F, F, T, T, T, T, F, T, T, F, F,  # 0x0X
    #                               ESC          
    F, F, F, F, F, F, F, F, F, F, F, T, F, F, F, F,  # 0x1X
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T,  # 0x2X 
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T,  # 0x3X 
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T,  # 0x4X 
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T,  # 0x5X 
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T,  # 0x6X 
    T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F,  # 0x7X 
    #             NEL                            
    X, X, X, X, X, T, X, X, X, X, X, X, X, X, X, X,  # 0x8X 
    X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X,  # 0x9X 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I,  # 0xaX 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I,  # 0xbX 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I,  # 0xcX 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I,  # 0xdX 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I,  # 0xeX 
    I, I, I, I, I, I, I, I, I, I, I, I, I, I, I, I   # 0xfX 
);

my $code_map = {
    'ASCII' => 'ascii',
    'EXT'   => 'non-iso extended ascii',
    'ISO'   => 'iso-8859',
    'UTF8'  => 'utf-8',
};


my $disable_guesser = sub {
    my ($self, $state, $cc) = @_;

    delete $state->{GUESS}{$cc};
    $state->{"NO_$cc"} = 1;
    return 0;
};


my $looks_ascii = sub {
    my ($self, $byte) = @_;
    my $state = $self->{CC};
    my $looks_ok = 0;

    return $looks_ok if ($state->{NO_ASCII});

    my $flag = $text_chars[$byte];
    if ($flag == T) {
        $state->{GUESS}{ASCII} = $looks_ok = 1;        
    } else {
        $self->$disable_guesser($state, 'ASCII');
    }

    return $looks_ok;
};


my $looks_latin1 = sub {
    my ($self, $byte) = @_;
    my $state = $self->{CC};
    my $looks_ok = 0;

    return $looks_ok if ($state->{'NO_ISO'});

    my $flag = $text_chars[$byte];
    if ($flag == T || $flag == I) {
        $state->{GUESS}{'ISO'} = $looks_ok = 1;
    } else {
        $self->$disable_guesser($state, 'ISO');
    }

    return $looks_ok;
};


my $looks_extended = sub {
    my ($self, $byte) = @_;
    my $state = $self->{CC};
    my $looks_ok = 0;

    return $looks_ok if ($state->{'NO_EXT'});

    my $flag = $text_chars[$byte];
    if ($flag == T || $flag == I || $flag == X) {
        $state->{GUESS}{'EXT'} = $looks_ok = 1;
    } else {
        $self->$disable_guesser($state, 'EXT');
    }

    return $looks_ok;
};


my $looks_utf8 = sub {
    my ($self, $byte) = @_;
    my $state = $self->{CC};
    my $looks_ok = 0;

    return $looks_ok if ($state->{'NO_UTF8'});

    if (!$state->{'UTF8_FOLLOW'}) {
        # We are looking at the first byte.

        $state->{GUESS}{'UTF8'} = $looks_ok = 1;

        if (($byte & 0x80) == 0) {
            # Even if the whole file is valid UTF-8 sequences,
            # still reject it if it uses weird control characters.
            if ($text_chars[$byte] != T) {
                $looks_ok = $self->$disable_guesser($state, 'UTF8');
            }

        } elsif (($byte & 0x40) == 0) {  # 10xxxxxx never 1st byte.
            $looks_ok = $self->$disable_guesser($state, 'UTF8');

        } else {
            delete $state->{'UTF8_BUF'};
            $state->{'UTF8_BUF'} = [];

            # Save byte for reinsertion into the input stream
            # should it failt to be valid utf8.
            push (@{$state->{'UTF8_BUF'}}, $byte);

            if (($byte & 0x20) == 0) {       # 110xxxxx
                $state->{'UTF8_FOLLOW'} = 1;
            } elsif (($byte & 0x10) == 0) {  # 1110xxxx 
                $state->{'UTF8_FOLLOW'} = 2;
            } elsif (($byte & 0x08) == 0) {  # 11110xxx
                $state->{'UTF8_FOLLOW'} = 3;
            } elsif (($byte & 0x04) == 0) {  # 111110xx 
                $state->{'UTF8_FOLLOW'} = 4;
            } elsif (($byte & 0x02) == 0) {  # 1111110x 
                $state->{'UTF8_FOLLOW'} = 5;
            } else {
                $looks_ok = $self->$disable_guesser($state, 'UTF8');
            }            
        }

    } else {
        # We are looking at secondary bytes.
        
        if (($byte & 0x80) == 0 || ($byte & 0x40)) {
            $looks_ok = $self->$disable_guesser($state, 'UTF8');

            # Reinsert tested bytes into input stream.
            unshift (@{$self->{CC}{BUFFER}}, @{$state->{'UTF8_BUF'}}, $byte);

            # Force next iteration of parent loop. (This is a
            # bit of a nasty I know).
            no warnings;
            next;

        } else {
            # Save byte for reinsertion into the input stream
            # should it failt to be valid utf8.
            push (@{$state->{'UTF8_BUF'}}, $byte);

            $state->{'UTF8_FOLLOW'} -= 1;
            $looks_ok = 1;
        }
    }

    return $looks_ok;
};


my $digest_ctx = sub {
    my ($self, $filename, $algorithm) = @_;

    $algorithm = $self->digest_algorithm unless ($algorithm);
    $algorithm = uc ($algorithm);

    my $class = join ('::', 'Digest', $algorithm);
    unless (eval "require $class") {
        die "unable to load module $class : $@";
    }

    my $ctx = $class->new;

    open (FILE, "<$filename")
        or die "unable to open file $filename for reading ($!)";

    binmode FILE;

    $ctx->addfile(*FILE);
    
    close (FILE);

    return $ctx;
};


# public interface

sub new {
    my $invocant = shift;
    my $class = ref ($invocant) || $invocant;
    my $self = {@_};

    bless ($self, $class);

    unless ($self->digest_algorithm) {
        $self->digest_algorithm('md5');
    }

    return $self;
}


# Accessor methods generated from closures.
# The closure reference is assigned to a
# typeglob of the appropriate name.

for my $property (qw(
    digest_algorithm
)) {
    my $slot   = $property;
    my $method = $property;
    $method =~ s/-/_/g;
    no strict "refs";       # so symbolic ref to typeglob works.
    *$method = sub {
        my $self = shift;
        $self->{$slot} = shift if @_;
        return $self->{$slot};
    }
}


sub charset {
    my ($self, $filename) = @_;
    my $cc;
    
    eval {
        delete $self->{CC};
        $self->{CC} = {};

        my $fh = $self->io_read($filename, 1);

        my $byte = 0;
        while ($fh->read($byte, 1)) {
            push (@{$self->{CC}{BUFFER}}, ord $byte);

            while ($byte = shift @{$self->{CC}{BUFFER}}) {
                   $self->$looks_ascii($byte)
                or $self->$looks_utf8($byte)
                or $self->$looks_latin1($byte)
                or $self->$looks_extended($byte);
            }
        }

        $fh->close;

        $cc = join (' or ', map {$code_map->{$_}} keys %{$self->{CC}{GUESS}});
        $cc or $cc = 'unknown';

    };
    if ($@) {
        close (FILE);
        die join ('', __PACKAGE__, '::charset() : ', $@);
    }

    return $cc;
}


sub compressed {
    my ($self, $filename, $extended) = @_;
    my @info = ();

    if (defined($extended) && $extended &&
        ($filename =~ m/\.tar\.gz$/i || $filename =~ m/\.tgz$/i)) {
        push (@info, 'tgz', 'tar xzf', 'tar czf', 'tar tzf');
    } elsif ($filename =~ m/\.(gz)$/i) {
        push (@info, $1, 'gzip -cd', 'gzip -9c', 'gzip -t');
    } elsif ($filename =~ m/\.(zip)$/i) {
        push (@info, $1, 'unzip -p', 'zip', 'unzip -tq');
    } elsif ($filename =~ m/\.(bz2)$/i) {
       push (@info, $1, 'bzip2 -cd', 'bzip2 -9c', 'bzip2 -t');
    } 

    return @info;
}


sub mime  {
    my ($self, $filename, $skip_archive) = @_;
    my $mime = '';

    $skip_archive //= 1;

    if ($skip_archive) {
        my @archive_info = $self->compressed($filename);
        if ($archive_info[0]) {
            $filename =~ s/\.$archive_info[0]$//i;
        }
    }
        
    if ($filename =~ m/\.([^.]+)$/) {
        $mime = $1;
    }

    return $mime;
};


sub digest {
    my ($self, $filename, $algorithm) = @_;
    my $digest;

    eval {
        $digest = $self->$digest_ctx($filename, $algorithm)->digest;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::digest() : ', $@);
    }

    return $digest;
}


sub hexdigest {
    my ($self, $filename, $algorithm) = @_;
    my $digest;

    eval {
        $digest = $self->$digest_ctx($filename, $algorithm)->hexdigest;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::hexdigest() : ', $@);
    }

    return $digest;
}


sub b64digest {
    my ($self, $filename, $algorithm) = @_;
    my $digest;

    eval {
        $digest = $self->$digest_ctx($filename, $algorithm)->b64digest;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::b64digest() : ', $@);
    }

    return $digest;
}


sub io_read {
    my ($self, $filename, $binmode) = @_;
    my $fh;

    eval {
        my @archive_info = $self->compressed($filename);
        if ($archive_info[1]) {
            $fh = new IO::Pipe
                or die join (' ', $filename, $!);
            $fh->reader("$archive_info[1] '$filename'")
                or die join (' ', $filename, $!);

        } else {
            $fh = new IO::File($filename, 'r')
                or die join (' ', $filename, $!);
        }

        if ($binmode) {
            if ($binmode =~ m/^:/) {
                # The $bimmode argument is interpreted as a layer directive.
                binmode ($fh, $binmode);
            } else {
                binmode ($fh);
            }
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::io_read() : ', $@);
    }

    return $fh;
}


sub io_write {
    my ($self, $filename, $mode, $binmode) = @_;
    my $fh;

    eval {
        $mode = '>' unless ($mode);

        my @archive_info = $self->compressed($filename);
        if ($archive_info[2]) {
            $fh = new IO::Pipe
                or die join (' ', $filename, $!);
            $fh->writer("$archive_info[2] $mode '$filename'")
                or die join (' ', $filename, $!);

        } else {
            $fh = new IO::File($filename, $mode)
                or die join (' ', $filename, $!);
        }

        if ($binmode) {
            if ($binmode =~ m/^:/) {
                # The $bimmode argument is interpreted as a layer directive.
                binmode ($fh, $binmode);
            } else {
                binmode ($fh);
            }
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::io_write() : ', $@);
    }

    return $fh;
}


sub size {
    my ($self, $filename) = @_;
    my $size;

    eval {
        $size = -s $filename;
    };
    if ($@) {
        die join ('', __PACKAGE__, '::size() : ', $@);
    }

    return $size;
}


sub disk_usage {
    my ($self, $dir, $human_readable) = @_;
    my $du;

    eval {
        (-e $dir)
            or die "no such file or directory: $dir";

        my $options = 's';
        if ($human_readable) {
            $options .= 'h';
        }
        
        $du = `du -$options $dir`;
        chomp ($du);
        ($du) = split (/\s+/, $du, 2);

        $du or die "unable to locate 'du' tool on this system";
    };
    if ($@) {
        die join ('', __PACKAGE__, '::disk_usage() : ', $@);
    }

    return $du;
}


sub copy {
    my ($self, $source, $destination, $append) = @_;
    
    eval {
        my $buf;
        my $from = $self->io_read($source, 1);
        my $to;
        if ($append) {
            $to = $self->io_write($destination, '>>', 1);
        } else {
            $to = $self->io_write($destination, '>', 1);
        }
        my $blocksize = (stat $from) [11] || 16384; # preferred block size.

        while (my $len = $from->sysread($buf, $blocksize)) {
            if (!defined $len) {
                next if $! == EINTR; # Interrupted by signal.
                die "system read error: $!";
            }

            my $offset = my $written = 0;
            while ($len) {   # handle partial writes.
                $written = $to->syswrite($buf, $len, $offset);
                die "system write error: $!" unless (defined $written);
                $offset += $written;
                $len    -= $written;
            }
        }

        $from->close();
        $to->close();
    };
    if ($@) {
        die join ('', __PACKAGE__, '::copy() : ', $@);
    }

    return $self;
}


sub move {
    my ($self, $source, $destination) = @_;

    eval {
        $self->copy($source, $destination);
        unlink ($source)
            or die "unable to unlink file $source";
    };
    if ($@) {
        die join ('', __PACKAGE__, '::move() : ', $@);
    }

    return $self;
}


sub compress {
    my ($self, $filename, $archive, $replace) = @_;

    eval {
        $self->compressed($archive)
            or die "archive $archive should specify a relevant mime type";

        if ($replace) {
            $self->move($filename, $archive);
        } else {
            $self->copy($filename, $archive);
        }

        $self->test_archive($archive)
            or die "archive validation failed for $archive";
    };
    if ($@) {
        die join ('', __PACKAGE__, '::compress() : ', $@);
    }

    return $self;
}


sub uncompress {
    my ($self, $archive, $filename, $replace) = @_;

    eval {
        $self->compressed($archive)
            or die "archive $archive should specify a relevant mime type";

        $self->test_archive($archive)
            or die "archive validation failed for $archive";

        if ($replace) {
            $self->move($archive, $filename);
        } else { 
            $self->copy($archive, $filename);
        }
        
    };
    if ($@) {
        die join ('', __PACKAGE__, '::uncompress() : ', $@);
    }

    return $self;
}


sub test_archive {
    my ($self, $filename) = @_;
    my $archive_ok = 1;

    eval {
        my @archive_info = $self->compressed($filename, 'extended');
        if ($archive_info[3]) {
            if (system ("$archive_info[3] '$filename' 2>&1")) {
                $archive_ok = 0;
            }
        }
    };
    if ($@) {
        die join ('', __PACKAGE__, '::test_archive() : ', $@);
    }

    return $archive_ok;
}

1;
__END__

=head1 NAME

Data::Fetcher::FileInfo - Module for retrieving information about files.

=head1 SYNOPSIS

use Data::Fetcher::FileInfo;

my $fi = new Data::Fetcher::FileInfo;

my $cc   = $fi->charset($filename);
my $mime = $fi->mime($filename);
my $sum  = $fi->digest($filename);
my $hex  = $fi->hexdigest($filename);
my $b64  = $fi->b64digest($filename);
my $siz  = $fi->size($filename);

=head1 DESCRIPTION

The Data::Fetcher::FileInfo class offers a set of helper functions to retrieve
certain information about files.

=head1 METHODS

=over 2

=item new()

  Data::Fetcher::FileInfo constructor.
  Creates a new Data::Fetcher::FileInfo object.

=item charset($filename)

  Returns as string a qualified guess about the character encoding of the
  specified file $filename. Will try to decompress archives on the fly.

  Encodings currently supported are 'ascii', 'iso-8859', 
  'non-iso extended ascii', 'utf-8' and 'unknown'.

  Throws exception on error.

=item mime($filename, $skip_archive)

  Returns as lowercase string the MIME type deducted from the 
  extension of the specified filename $filename.

  If $skip_archive is set to true (default) archive mime types
  like 'gz' and 'zip' will not be reported.

=item compressed($filename)

  Returns a four-element list if the filename specified by $filename 
  indicates compression, otherwise the empty list is returned.

  $LIST[0] = archive mime type.
  $LIST[1] = uncompress command to be used in a piped read.
  $LIST[2] = compress command to be used in a piped write.
  $LIST[3] = archive test command.

=item io_read($filename, $binmode)

  Returns IO::Handle subclass enabling read operations on file
  pointed to by $filename.

  If file pointed to by $filename is compressed an IO::Pipe object
  is returned forcing decompression on the fly during reading, and no file
  seek operation are available. Otherwise an IO::File object is
  returned.

  If $binmode is set to a true value, the handle will be read in
  'binary' mode. If the $binmode argument contains a string that
  starts with a colon, e.g. ':utf8', it is interpreted as an I/O layer 
  directive which is then used to control the behaviour of the file
  handle.

  Throws exception on error.

=item io_write($filename, $mode, $binmode)

  Returns IO::Handle subclass enabling write operations on file
  specified by $filename.

  If specified filename indicates compression an IO::Pipe object
  is returned forcing compression on the fly during writing, and no file
  seek operation are available. Otherwise an IO::File object is
  returned.

  The optional mode $mode indicates how the file is opened. Mode can be
  one of '<', '>', '>>', '+<', '+>' or '+>>' as supported by the perl
  open() system call. Using on-the-fly compression only '>' and '>>' are
  available though.

  If $binmode is set to a true value, the handle will be written in
  'binary' mode. If the $binmode argument contains a string that
  starts with a colon, e.g. ':utf8', it is interpreted as an I/O layer 
  directive which is then used to control the behaviour of the file
  handle.

  Throws exception on error.

=item copy($source, $destination, $append)

  Performs a binary copy of file given by $source to destination given 
  by $destination.

  if $append is set to a true value the destination file will not be
  truncated if it already exists and source file content will be added
  to the end of the file.

  Throws exception on error.

=item move($source, $destination)

  Performs a binary move of file given by $source to destination given 
  by $destination.

  Throws exception on error.

=item compress($filename, $archive, $replace)

  Compresses file given by $filename into archive given by $archive.

  If $replace is set to a true value the orignal source file will be
  unlinked upon completion.

=item uncompress($archive, $filename, $replace)

  Uncompresses archive given by $archive into file given by $filename.

  If $replace is set to a true value the orignal archive file will be
  unlinked upon completion.

=item test_archive($filename)

  Returns true value if file pointed to by $filename is a valid compressed
  archive or isn't compressed at all.

=item digest($filename, $algorithm)

  Calculates the digest of the content of the file pointed to by $filename
  using the algorithm $algorithm. Default algorithm is md5.

  The returned binary form string will be 16 bytes long.

  Throws exception on error.

=item hexdigest($filename, $algorithm)

  Calculates the digest of the content of the file pointed to by $filename
  using the algorithm $algorithm. Default algorithm is md5.

  The returned digest will be in hexadecimal form with a length of 32.

  Throws exception on error.

=item b64digest($filename, $algorithm)

  Calculates the digest of the content of the file pointed to by $filename
  using the algorithm $algorithm. Default algorithm is md5.

  The returned digest will be in a base64 encoded string with a length of 22.

  Throws exception on error.

=item digest_algorithm([$algorithm])

  Gets or sets the digest algorithm.

=item size($filename)

  Returns the size of the file pointed to by $filename.

  Throws exception on error.

=item disk_usage($dir, [$human_readable])

  Returns estimated file space usage (in kilobytes) of the file or directory
  pointed to by $dir, recursively for directories.

  If the $human_readable flag is set, the size returned is a string in 
  human readable format (e.g., 1K 234M 2G).

  Throws exception on error.

=back

=head1 CAVEATS

The appending output mode '>>' for io_write() will fail to generate a valid
archive if on-the-fly zip compression is used.

=head1 AUTHOR

Jan Bauer Nielsen, E<lt>jbn@dtic.dtu.dkE<gt>
Morten Rønne, E<lt>mron@dtic.dtu.dkE<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

