#!/usr/bin/perl
use strict;
use warnings;
use Test::More qw/no_plan/;
use Mixi::HS;
use Data::Dumper;

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
    password : "" 
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
    $dbh->do("DROP TABLE if exists hs_test_0$_.foo");
    $dbh->do(
        qq{
        create table if not exists hs_test_0$_.foo (
            id int unsigned not null auto_increment,
            name varchar(255) not null,
            age int unsigned not null,
            gender enum('m', 'f') not null,
            seq int unsigned not null,
            primary key (id),
            key (name, age),
            unique key (seq)
        ) ENGINE=Innodb DEFAULT CHARSET=UTF8;
    }
    );
    $dbh->do("DROP TABLE if exists hs_test_0$_.links");
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
}

eval {
    Mixi::HS->init_string( "MyApp", $yaml );
    my $foo = Mixi::HS->load('Test::Foo');
    is( $foo->shard_count, 2 );
    foreach ( 0 .. $foo->shard_count - 1 ) {
        $foo->switch_shard($_);
        $foo->fast_create(
            {
                name   => "renchenyu$_",
                age    => 30,
                gender => "m",
                seq    => 1,
            }
        );
        eval {
            $foo->fast_create_ignore(
                {
                    id     => 1,
                    name   => "renchenyu$_",
                    age    => 30,
                    gender => "m",
                    seq    => 2,
                }
            );
        };
        ok( 1, "fast_create_ignore with duplicate primary key" ) unless $@;
        eval {
            $foo->fast_create_ignore(
                {
                    id     => 100,
                    name   => "ab",
                    age    => 30,
                    gender => "m",
                    seq    => 1
                }
            );
        };
        ok( 1, "fast_create_ignore with duplicate unique key" ) unless $@;

        $foo->fast_create_or_update(
            {
                id     => 1,
                name   => "renchenyu$_",
                age    => "10",
                gender => "f",
                seq    => 1,
            }
        );
        is_deeply(
            $foo->find_by_id( [1] ),
            {
                id     => 1,
                name   => "renchenyu$_",
                age    => "10",
                gender => "f",
                seq    => 1,
            },
            "fast_create_or_update"
        );
        my $id = $foo->create(
            {
                name   => "renchenyu$_",
                age    => 30,
                gender => "f",
                seq    => 2,
            }
        );
        is( $id, 2 );
        is_deeply(
            $foo->find_by_name_age( ["renchenyu$_"], 1, 0 ),
            {
                id     => 1,
                name   => "renchenyu$_",
                age    => 10,
                gender => "f",
                seq    => 1
            },
            "find_by_name_age, single value"
        );
        is_deeply(
            $foo->find_by_name_age( ["renchenyu$_"], 2, 0 ),
            [
                {
                    id     => 1,
                    name   => "renchenyu$_",
                    age    => 10,
                    gender => "f",
                    seq    => 1
                },
                {
                    id     => 2,
                    name   => "renchenyu$_",
                    age    => 30,
                    gender => "f",
                    seq    => 2
                }
            ],
            "find_by_name_age, count"
        );
        is_deeply(
            $foo->find_by_name_age( [ "renchenyu$_", 30 ], 1, 0 ),
            {
                id     => 2,
                name   => "renchenyu$_",
                age    => 30,
                gender => "f",
                seq    => 2
            },
            "find_by_name_age, full value"
        );
        is( $foo->find_by_name_age( [ "renchenyu$_", 40 ], 1, 0 ),
            undef, "no value" );

        is( $foo->update_by_id( { name => "aaa" }, [2] ), 1, "update_by_id" );
        is( $foo->find_by_id( [2] )->{name},
            "aaa", "update_by_id successfully" );
        is( $foo->update_by_name_age( { name => "bbb" }, ["aaa"] ),
            1, "update_by_name_age, only name" );
        is( $foo->find_by_id( [2] )->{name},
            "bbb", "update_by_name_age, only name, successfully" );
        is( $foo->update_by_name_age( { name => "ccc" }, [ "bbb", 30 ] ),
            1, "update_by_name_age" );
        is( $foo->find_by_id( [2] )->{name},
            "ccc", "update_by_name_age successfully" );

        is( $foo->delete_by_name_age( ["ccc"] ), 1, "delete_by_name_age" );
        is( $foo->delete_by_id(       [1] ),     1, "delete_by_id" );

        #normal sql
        is( $foo->count, 0, "count zero" );
        is(
            $foo->create(
                {
                    name   => "xiaobao",
                    age    => 10,
                    gender => "m",
                    seq    => 1
                }
            ),
            3,
            "create by sql"
        );
        is_deeply($foo->search({name=>"xiaobao"}), [{id => 3, name => "xiaobao", gender => "m", seq => 1, age => 10}], "search by sql, check create");
        is($foo->update({name=>"jiong"}, {name=>"xiaobao"}), 1, "update by sql");
        is_deeply($foo->search({name=>"jiong"}), [{id => 3, name => "jiong", gender => "m", seq => 1, age => 10}], "search by sql, check search");
        is($foo->delete({id => 3}), 1, "delete by sql");
        is($foo->search({id=>3})->[0], undef, "search by sql, check delete");

    }

};
print $@ if $@;

