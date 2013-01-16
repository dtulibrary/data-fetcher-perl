#!/usr/bin/perl
use Module::Build;
my $build = Module::Build->new
  (
    module_name => 'Data::Fetcher',
    license => 'perl',
    requires => {
        'perl' => '5.8.0',
        'File::Copy' => '2.11',
        'File::Listing' => '5.810',
        'HTTP::OAI' => '3.20',
        'Net::FTP' => '2.77',
        'Net::SFTP::Foreign' => '1.42',
        'XML::LibXML' => '1.58',
        'DateTime' => '0.06',
        'DateTime::Format::ISO8601' => '0.06',
        'LWP' => '5.836',
        'Net::FTPSSL' => '0.20',
        'Test/Distribution.pm' => 0,
    },
);
$build->create_build_script;
