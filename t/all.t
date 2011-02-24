#!/usr/bin/perl
use strict;
use warnings;
use Test::More qw/no_plan/;
use Mixi::HS;

use DBI;

my $dbh = eval {
    DBI->connect( "DBI:mysql:database=test;host=localhost;port=3306",
        "root", "", { RaiseError => 1, } );
};
if ($@) {
    print "Cannot connect to localhost:3306:test -- $@\n";
    exit;
}

my $yaml = <<END;
DEFAULT :
    master : "localhost:3307"
    slave :
        - "localhost:3307"
        - "localhost:3307"
    user : "root"
    password : "cjnyhwm1" 
    hs_port : 9998
    hs_port_w : 9999

"Test::.*" :
    -
        master : "localhost:3306"
        slave :
            - "localhost:3306"
            - "localhost:3306"
        database : "hs_test_01"
    - 
        master : "localhost:3306"
        slave :
            - "localhost:3306"
            - "localhost:3306"
        database : "hs_test_02"
END

foreach ( 1 .. 2 ) {
    $dbh->do(
        qq{
        create database if not exists hs_test_0$_
    }
    );
}

foreach ( 1 .. 2 ) {
    $dbh->do(
        qq{
        create table if not exists hs_test_0$_.foo (
            id int unsigned not null auto_increment,
            name varchar(255) not null,
            age int unsigned not null,
            gender enum('m', 'f') not null,
            primary key (id),
            key (name, age)
        ) ENGINE=Innodb DEFAULT CHARSET=UTF8;
    }
    );
    $dbh->do(
        qq{
        truncate hs_test_0$_.foo;
    }
    );
    $dbh->do(
        qq{
        create table if not exists hs_test_0$_.links (
            a_id int unsigned not null,
            b_id int unsigned not null,
            updated_at timestamp not null,
            primary key (a_id, b_id)
        ) ENGINE=Innodb DEFAULT CHARSET=UTF8;
    }
    );
    $dbh->do(
        qq{
        truncate hs_test_0$_.links;
    }
    );
}

eval {
    Mixi::HS->init_string( "MyApp", $yaml, 1 );
    my $foo = Mixi::HS->load('Test::Foo');
    is( $foo->shard_count, 2 );
    foreach ( 0 .. $foo->shard_count - 1 ) {
        $foo->switch_shard($_);
        $foo->new(
            {
                name   => "renchenyu$_",
                age    => 30,
                gender => "m"
            }
        )->create;
        my $id = $foo->new(
            {
                name   => "renchenyu$_",
                age    => 30,
                gender => "f"
            }
        )->create_auto_inc;

        is( $id, 2 );
        is_deeply( $foo->find_by_pk( [1] )->to_hash(),
            { id => 1, name => "renchenyu$_", age => 30, gender => "m" } );
        is_deeply(
            $foo->find_by_name_age( ["renchenyu$_"], 1, 0 )->to_hash(),
            { id => 1, name => "renchenyu$_", age => 30, gender => "m" }
        );
        is_deeply(
            [
                map { $_->to_hash }
                  @{ $foo->find_by_name_age( ["renchenyu$_"], 2, 0 ) }
            ],
            [
                { id => 1, name => "renchenyu$_", age => 30, gender => "m" },
                { id => 2, name => "renchenyu$_", age => 30, gender => "f" }
            ]
        );
        is_deeply(
            $foo->find_by_name_age( [ "renchenyu$_", 30 ], 1, 0 )->to_hash(),
            { id => 1, name => "renchenyu$_", age => 30, gender => "m" }
        );
        is( $foo->find_by_name_age( [ "renchenyu$_", 40 ], 1, 0 ), undef );

        my $a = $foo->find_by_pk([1]);
        $a->age(50);
        is($a->update, 1);
        is_deeply( $foo->find_by_pk( [1] )->to_hash(),
            { id => 1, name => "renchenyu$_", age => 50, gender => "m" } );
        is($a->delete, 1);
        is($foo->find_by_pk([1]), undef);
    }

};
print $@ if $@;

