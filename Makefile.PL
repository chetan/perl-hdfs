use inc::Module::Install::DSL 0.91;
all_from lib/HDFS.pm
requires Moose
requires IO::CaptureOutput
requires File::Basename
test_requires Test::More 0.42;
license 'perl';
