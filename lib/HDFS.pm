package HDFS;

our $VERSION = 0.01;

use Moose;
use IO::CaptureOutput qw(capture_exec);

use HDFS::Dir;
use HDFS::File;

has 'home' => (
    is       => 'ro',
    default  => $ENV{HADOOP_HOME},
);

has 'uri' => (
    is       => 'ro',
    default  => "hdfs://localhost:9000",
);

has 'user' => ( is => 'rw' );

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
    if ($self->user) {
        $c = "sudo -u ${\$self->user} $c";
    }
    print "cmd: $c\n";

    my($stdout, $stderr, $success, $exit_code) = capture_exec($c);
    return (stdout => $stdout, stderr => $stderr, success => $success, exit_code => $exit_code);
}

sub path {
    my($self, $path) = @_;
    if (substr($path,0,1) ne "/") {
        $path = "/$path";
    }
    return $self->uri . $path;
}

sub ls {
    my($self, $path) = @_;
    $path ||= "/";

    return HDFS::Dir->new(hdfs => $self, path => $path)->ls();
}

sub ls_r {
    my($self, $path) = @_;
    $path ||= "/";

    return HDFS::Dir->new(hdfs => $self, path => $path)->ls_r();
}

sub copyToLocal {
    my($self, $src, $dest) = @_;

    return HDFS::File->new(hdfs => $self, path => $src)->copyToLocal($dest);
}

sub rm {
    my($self, $path) = @_;

    return HDFS::File->new(hdfs => $self, path => $path)->rm();
}

sub rm_r {
    my($self, $path) = @_;

    return HDFS::Dir->new(hdfs => $self, path => $path)->rm_r();
}

1;
