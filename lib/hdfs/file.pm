package HDFS::File;

use Moose;
use Data::Dumper;

# { permissions => $perm, replicas => $repl, user => $user, group => $group,
               # size => $size, date => $date, time => $time, filename => join(" ", @file),
               # path => $self->path, }

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

has 'permissions' => ( is => 'ro' );
has 'replicas'    => ( is => 'ro' );
has 'user'        => ( is => 'ro' );
has 'group'       => ( is => 'ro' );
has 'size'        => ( is => 'ro' );
has 'date'        => ( is => 'ro' );
has 'time'        => ( is => 'ro' );
has 'filename'    => ( is => 'ro' );

sub is_file {
    my $self = shift;
    return $self->permissions !~ /^d/ ? 1 : 0;
}

sub is_dir {
    my $self = shift;
    return $self->permissions =~ /^d/ ? 1 : 0;
}


1;
