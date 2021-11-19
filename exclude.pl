#!/bin/sh
#! -*-perl-*-
eval 'exec perl -x -T -w $0 ${1+"$@"}'
  if 0;
# This script takes null delimited files as input
# it drops paths that match the listed exclusions
# output is null delimited to match input
use File::Basename;

my $dirname = dirname(__FILE__);
my $exclude_file = $dirname.'/excludes.txt';
my $only_file = $dirname.'/only.txt';

sub file_to_re {
  my ($file, $fallback) = @_;
  my @items;
  if (-e $file) {
    open FILE, '<', $file;
    local $/=undef;
    my $file=<FILE>;
    for (split /\R/, $file) {
      next if /^#/;
      s/^\s*(.*)\s*$/(?:$1)/;
      push @items, $_;
    }
  }
  my $pattern = scalar @items ? join "|", @items : $fallback;
  return $pattern;
}

my $exclude = file_to_re($exclude_file, '^$');
my $only = file_to_re($only_file, '.');

$/="\0";
while (<>) {
  chomp;
  next if m{$exclude};
  next unless m{$only};
  print "$_$/";
}
