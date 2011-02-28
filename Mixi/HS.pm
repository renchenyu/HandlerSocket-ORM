package Mixi::HS;
use strict;
use warnings;

use Module::Find;
use YAML::XS;
use Carp qw/confess/;

use Mixi::HS::DBInfo;
use Data::Dumper;

my %models;
my $g_namespace;

sub init_hash {
    my ( $class, $namespace, $yaml_config, $is_debug ) = @_;

    $g_namespace = $namespace;
    my $default = delete $yaml_config->{DEFAULT};

    my @found = eval { useall $namespace };
    confess $@ if $@;

    foreach my $module (@found) {
        print STDERR "init $module\n" if $is_debug;
        my $config = [ {} ];
        foreach my $key ( keys %$yaml_config ) {
            if ( $module =~ /^${namespace}::$key$/ ) {
                my $c = $yaml_config->{$key};
                $config = ref $c eq 'ARRAY' ? $c : [$c];
                last;
            }
        }
        my @params;
        foreach (@$config) {
            push @params, Mixi::HS::DBInfo->new( { %$default, %$_ } );
        }
        $models{$module} = $module->init( \@params, $is_debug );
    }
}

sub init_string {
    my ( $class, $namespace, $yaml_string, $is_debug ) = @_;

    my $yaml_config = eval { YAML::XS::Load($yaml_string) };
    confess $@ if $@;

    $class->init_hash( $namespace, $yaml_config, $is_debug );
}

sub init_file {
    my ( $class, $namespace, $yaml_file, $is_debug ) = @_;

    my $yaml_config = eval { YAML::XS::LoadFile($yaml_file) };
    confess $@ if $@;

    $class->init_hash( $namespace, $yaml_config, $is_debug );
}

sub load {
    my $class = shift;
    my ($name) = @_;

    my $c = $models{ $g_namespace . "::" . $name };
    die "Cannot find ${g_namespace}::$name" if !defined $c;

    return $c;
}

1;
