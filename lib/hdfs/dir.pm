package HDFS::Dir;

use Moose;

extends 'HDFS::File';

use IO::CaptureOutput qw(capture_exec);
use Data::Dumper;


sub ls {
    my($self) = @_;
    my $path = $self->hdfs->path($self->path);

    my %ret = $self->hdfs->run_cmd("fs -ls $path");
    $ret{success} || die("unable to list: " . $ret{stdout});

    return $self->_parse_files($ret{stdout});
}

sub ls_r {
    my($self) = @_;
    my $path = $self->hdfs->path($self->path);

    my %ret = $self->hdfs->run_cmd("fs -lsr $path");
    $ret{success} || die("unable to list: " . $ret{stdout});

    return $self->_parse_files($ret{stdout});
}

sub _parse_files {
    my($self, $str) = @_;

    my $files = [];
    my @lines = split(/\n/, $str);
    my $c = 0;
    for my $line (@lines) {
        $c++;
        if ($c == 1) {
            next;
        }

        # permissions number_of_replicas userid groupid filesize modification_date modification_time filename
        my($perm, $repl, $user, $group, $size, $date, $time, @file) = split(/\s+/, $line);

        my $args = { permissions => $perm, replicas => $repl, user => $user, group => $group,
                     size => $size, date => $date, time => $time, filename => join(" ", @file),
                     path => $self->path, hdfs => $self->hdfs };

        if ($perm =~ /^d/) {
            push @$files, HDFS::Dir->new($args);
        } else {
            push @$files, HDFS::File->new($args);
        }

    }
    return $files;
}

1;
