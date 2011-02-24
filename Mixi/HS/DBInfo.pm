package Mixi::HS::DBInfo;
use strict;
use warnings;
use base qw/Class::Accessor::Fast/;

#{
#       master => "localhost:3306",
#       slave => ["localhost:3306", "localhost:3307"],
#       database => "test",
#       user => "root"
#       password => ""
#       hs_port => 9998
#       hs_port_w => 9999
#}

__PACKAGE__->mk_accessors(qw/master slave database user password hs_port hs_port_w/);

1;
