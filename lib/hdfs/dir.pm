package HDFS::Dir;

use Moose;
use IO::CaptureOutput qw(capture_exec);
use Data::Dumper;

has 'hdfs' => (
    is       => 'ro',
    isa      => 'HDFS',
    init_arg => 'hdfs',
);

has 'path' => (
    is       => 'ro',
    isa      => 'Str',
    init_arg => 'path',
);

sub ls {
    my($self) = @_;
    my $path = $self->hdfs->path($self->path);
    
    my %ret = $self->hdfs->run_cmd("fs -ls $path");
    $ret{success} || die("unable to list: " . $ret{stdout});
    
    my $files = [];
    my @lines = split(/\n/, $ret{stdout});
    my $c = 0;
    for my $line (@lines) {
        $c++;
        if ($c == 1) {
            next;
        }
        
        # permissions number_of_replicas userid groupid filesize modification_date modification_time filename
        my($perm, $repl, $user, $group, $size, $date, $time, @file) = split(/\s+/, $line);
        
        push @$files, { permissions => $perm, replicas => $repl, user => $user, group => $group, 
                       size => $size, date => $date, time => $time, filename => join(" ", @file),
                       path => $self->path, }
                       
    }
    return $files;
}

1;
