package Mixi::HS::Exception;
use strict;
use warnings;

use base qw/Class::Accessor::Fast/;
use Carp qw/confess/;

__PACKAGE__->mk_accessors(qw/error/);

sub throw {
    my $self = shift;
    confess $self;
}

1;
