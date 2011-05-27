package HDFS;

use Moose;
use Data::Dumper;
use IO::CaptureOutput qw(capture_exec);

use HDFS::Dir;
use HDFS::File;

has 'home' => (
    is       => 'ro',
    init_arg => 'home',
    default  => $ENV{HADOOP_HOME},
);

has 'uri' => (
    is       => 'ro',
    default  => "hdfs://localhost:9000",
    init_arg => 'uri',
);

sub BUILD {
    my $self = shift;

    my($stdout, $stderr, $success, $exit_code) = capture_exec($self->cmd);
    $exit_code == -1 && die("unable to find 'hadoop' command; set HADOOP_HOME or pass 'home' argument to constructor");
}

sub cmd {
    my $self = shift;
    my $cmd;
    if ($self->home) {
        $cmd = $self->home . "/bin/";
    }
    $cmd .= "hadoop";
    return $cmd;
}

sub run_cmd {
    my($self, $cmd) = @_;
    my $c = $self->cmd . " " . $cmd;
    print "cmd: $c\n";
    my($stdout, $stderr, $success, $exit_code) = capture_exec($c);
    return (stdout => $stdout, stderr => $stderr, success => $success, exit_code => $exit_code);
}

sub path {
    my($self,$path) = @_;
    if (substr($path,0,1) ne "/") {
        $path = "/$path";
    }
    return $self->uri . $path;
}

sub ls {
    my($self,$path) = @_;
    $path ||= "/";

    return HDFS::Dir->new(hdfs => $self, path => $path)->ls();
}

1;
