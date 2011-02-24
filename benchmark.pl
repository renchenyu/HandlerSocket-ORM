#!/usr/bin/perl -w
use strict;
use warnings;

use Mixi::HS;
use DBI;
use Benchmark qw/:all/;

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

Mixi::HS->init_string( "MyApp", $yaml);
my $foo = Mixi::HS->load('Test::Foo');

timethese(10000, {
    "hs_insert" => sub {
        $foo->switch_shard(0);
        $foo->new({
            name => "name",
            gender => "m",
            age => 20
        })->create
    },
    "sql_insert" => sub {
        $foo->switch_shard(1);
        $foo->new({
            name => "name",
            gender => "m",
            age => 20
        })->create_auto_inc;
    }
});

my @randoms1;
push @randoms1, int(rand(9990)) + 1 for (1..10000);
my @randoms2 = @randoms1;

timethese(10000, {
    "hs_pk" => sub {
        $foo->switch_shard(0);
        my $r = $foo->find_by_pk([shift @randoms1]);
    },
    "sql_pk" => sub {
        $foo->switch_shard(1);
        my $r = $foo->search({ id => shift @randoms2 })->[0];
    },
});
