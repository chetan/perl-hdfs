package HDFS::Dir;

use Moose;

extends 'HDFS::File';

use IO::CaptureOutput qw(capture_exec);
use Data::Dumper;
use File::Basename qw(fileparse);

sub ls {
    my($self) = @_;

    my %ret = $self->hdfs->run_cmd("fs -ls ${\$self->uri}");
    $ret{success} || die("unable to ls ${\$self->uri}: " . $ret{stdout});

    return $self->_parse_files($ret{stdout});
}

sub ls_r {
    my($self) = @_;

    my %ret = $self->hdfs->run_cmd("fs -lsr ${\$self->uri}");
    $ret{success} || die("unable to lsr ${\$self->uri}: " . $ret{stdout});

    return $self->_parse_files($ret{stdout});
}

sub rm_r {
    my $self = shift;

    my %ret = $self->hdfs->run_cmd("fs -rmr ${\$self->uri}");
    $ret{success} || die("unable to rmr ${\$self->uri}: " . $ret{stdout} . "\n" . $ret{stderr});

    return 1;
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

        my($f, $p) = fileparse(join(" ", @file));

        my $args = { permissions => $perm, replicas => $repl, user => $user, group => $group,
                     size => $size, date => $date, time => $time, file => $f,
                     path => $p, hdfs => $self->hdfs };

        if ($perm =~ /^d/) {
            push @$files, HDFS::Dir->new($args);
        } else {
            push @$files, HDFS::File->new($args);
        }

    }
    return $files;
}

1;
