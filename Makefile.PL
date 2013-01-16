#!/usr/bin/perl
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME => 'Data::Fetcher',
    VERSION => '1.0.2',
    PREREQ_PM         => {
        'File::Copy' => 2.11,
        'File::Listing' => 5.810,
        'HTTP::OAI' => 3.20,
        'Net::FTP' => 2.77,
        'Net::SFTP::Foreign' => 1.42,
        'XML::LibXML' => 1.58,
        'DateTime' => 0.06,
        'DateTime::Format::ISO8601' => 0.06,
        'LWP' => 5.836,
        'Net::FTPSSL' => 0.20,
    },
    ABSTRACT => 'Data retrieval modules.',
    AUTHOR   => 'Jan Bauer Nielsen <jbn@dtic.dtu.dk>, Morten Rønne <mron@dtic.dtu.dk>',
);
