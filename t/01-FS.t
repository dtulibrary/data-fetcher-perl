use strict;
use warnings;

use Test;
use File::Temp ();

BEGIN { plan tests => 5 };

my ($fs, $files, $fh, $text);

use Data::Fetcher::FS;

# Look for our own test file
$fs = Data::Fetcher::FS->new(
  'directory' => 't',
  'file-filter' => '^01-FS\.t$',
);
$files = $fs->list_files();
ok(scalar(@$files) == 1);
ok($files->[0]->pi eq 't/01-FS.t');

$fs = Data::Fetcher::FS->new(
  'directory' => 't',
  'directory-filter' => '^include$',
  'file-filter' => '^extra$'
);
$files = $fs->list_files();
ok(scalar(@$files) == 1);
ok($files->[0]->pi eq 't/include/extra');

$fh = File::Temp->new();
$fs->get_file($files->[0], $fh->filename);
open(IN, '<', $fh->filename);
$text = join('', <IN>);
ok($text eq "Include this\n");
undef $fh;
