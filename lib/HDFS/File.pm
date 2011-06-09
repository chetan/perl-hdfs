package HDFS::File;

use Moose;
use Data::Dumper;

has 'hdfs' => (
   is       => 'ro',
   isa      => 'HDFS',
);

has 'path' => (
   is       => 'ro',
   isa      => 'Str',
);

has 'file' => (
   is       => 'ro',
   isa      => 'Str',
   default  => '',
);

has 'permissions' => ( is => 'ro' );
has 'replicas'    => ( is => 'ro' );
has 'user'        => ( is => 'ro' );
has 'group'       => ( is => 'ro' );
has 'size'        => ( is => 'ro' );
has 'date'        => ( is => 'ro' );
has 'time'        => ( is => 'ro' );

sub is_file {
    my $self = shift;
    return $self->permissions !~ /^d/ ? 1 : 0;
}

sub is_dir {
    my $self = shift;
    return $self->permissions =~ /^d/ ? 1 : 0;
}

sub filename {
    my $self = shift;
    return $self->path . $self->file;
}

sub uri {
    my $self = shift;
    return $self->hdfs->path($self->filename);;
}

sub copyToLocal {
    my($self, $dest) = @_;

    my %ret = $self->hdfs->run_cmd("fs -copyToLocal ${\$self->uri} $dest");
    $ret{success} || die("unable to copy ${\$self->uri} to $dest: " . $ret{stdout} . "\n" . $ret{stderr});

    return 1;
}

sub rm {
    my $self = shift;

    my %ret = $self->hdfs->run_cmd("fs -rm ${\$self->uri}");
    $ret{success} || die("unable to rm ${\$self->uri}: " . $ret{stdout} . "\n" . $ret{stderr});

    return 1;
}

1;
