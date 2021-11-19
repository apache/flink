#!/bin/sh
#! -*-perl-*-
eval 'exec perl -x -T -w $0 ${1+"$@"}'
  if 0;

use File::Path qw(remove_tree);

my %letter_map;

sub get_file_from_env {
  my ($var, $fallback) = @_;
  return $fallback unless defined $ENV{$var};
  $ENV{$var} =~ /(.*)/;
  return $1;
}

my $warning_output = get_file_from_env('warning_output', '/dev/stderr');
my $more_warnings = get_file_from_env('more_warnings', '/dev/stderr');
my $should_exclude_file = get_file_from_env('should_exclude_file', '/dev/null');

my @delayed_warnings;
open WARNING_OUTPUT, '>', $warning_output;
open MORE_WARNINGS, '>', $more_warnings;
open SHOULD_EXCLUDE, '>', $should_exclude_file;

sub get_field {
  my ($record, $field) = @_;
  return undef unless $record =~ (/\b$field:\s*(\d+)/);
  return $1;
}

my %expected = ();
sub expect_item {
  my ($item, $value) = @_;
  return 0 unless defined $expected{$item};
  $expected{$item} ||= $value;
  return $value;
}

sub skip_item {
  my ($word) = @_;
  return 1 if expect_item($word, 1);
  my $key = lc $word;
  return 2 if expect_item($key, 2);
  if ($key =~ /.s$/) {
    if ($key =~ /ies$/) {
      $key =~ s/ies$/y/;
    } else {
      $key =~ s/s$//;
    }
  } elsif ($key =~ /.[^aeiou]ed$/) {
    $key =~ s/ed$//;
  } else {
    return 0;
  }
  return 3 if expect_item($key, 3);
  return 0;
}

my @directories;
my @cleanup_directories;

my %unknown;

for my $directory (<>) {
  chomp $directory;
  next unless $directory =~ /^(.*)$/;
  $directory = $1;
  unless (-e $directory) {
    print STDERR "Could not find: $directory\n";
    next;
  }
  unless (-d $directory) {
    print STDERR "Not a directory: $directory\n";
    next;
  }
  # stats isn't written if all words in the file are in the dictionary
  next unless (-s "$directory/stats");

  # if there's no filename, we can't report
  next unless open(NAME, '<', "$directory/name");
  my $file=<NAME>;
  close NAME;
  my ($words, $unrecognized, $unknown, $unique);

  {
    open STATS, '<', "$directory/stats";
    my $stats=<STATS>;
    close STATS;
    $words=get_field($stats, 'words');
    $unrecognized=get_field($stats, 'unrecognized');
    $unknown=get_field($stats, 'unknown');
    $unique=get_field($stats, 'unique');
    #print STDERR "$file (unrecognized: $unrecognized; unique: $unique; unknown: $unknown, words: $words)\n";
  }

  # These heuristics are very new and need tuning/feedback
  if (
      ($unknown > $unique)
      # || ($unrecognized > $words / 2)
  ) {
    push @delayed_warnings, "$file: line 1, columns 1-1, Warning - Skipping `$file` because there seems to be more noise ($unknown) than unique words ($unique) (total: $unrecognized / $words). (noisy-file)\n";
    print SHOULD_EXCLUDE "$file\n";
    push @cleanup_directories, $directory;
    next;
  }
  unless (-s "$directory/unknown") {
    push @cleanup_directories, $directory;
    next;
  }
  open UNKNOWN, '<', "$directory/unknown";
  for $token (<UNKNOWN>) {
    chomp $token;
    $token =~ s/^[^Ii]?'+(.*)/$1/;
    $token =~ s/(.*?)'+$/$1/;
    next unless $token =~ /./;
    my $key = lc $token;
    $key =~ s/''+/'/g;
    $key =~ s/'[sd]$//;
    my $char = substr $key, 0, 1;
    $letter_map{$char} = () unless defined $letter_map{$char};
    my %word_map = ();
    %word_map = %{$letter_map{$char}{$key}} if defined $letter_map{$char}{$key};
    $word_map{$token} = 1;
    $letter_map{$char}{$key} = \%word_map;
  }
  close UNKNOWN;
  push @directories, $directory;
}
close SHOULD_EXCLUDE;

# load expect
if (defined $ENV{'expect'}) {
  $ENV{'expect'} =~ /(.*)/;
  my $expect = $1;
  if (open(EXPECT, '<', $expect)) {
    while ($word = <EXPECT>) {
      chomp $word;
      $expected{$word} = 0;
    }
    close EXPECT;
  }
}

my %seen = ();

for my $directory (@directories) {
  next unless (-s "$directory/warnings");
  next unless open(NAME, '<', "$directory/name");
  my $file=<NAME>;
  close NAME;
  open WARNINGS, '<', "$directory/warnings";
  for $warning (<WARNINGS>) {
    chomp $warning;
    $warning =~ s/(line \d+) cols (\d+-\d+): '(.*)'/$1, columns $2, Warning - `$3` is not a recognized word. (unrecognized-spelling)/;
    my ($line, $range, $item) = ($1, $2, $3);
    next if skip_item($item);
    if (defined $seen{$item}) {
      print MORE_WARNINGS "$file: $warning\n";
    } else {
      $seen{$item} = 1;
      print WARNING_OUTPUT "$file: $warning\n";
    }
  }
  close WARNINGS;
}
close MORE_WARNINGS;

for my $warning (@delayed_warnings) {
  print WARNING_OUTPUT $warning;
}
close WARNING_OUTPUT;

# group related words
for my $char (sort keys %letter_map) {
  for my $plural_key (sort keys(%{$letter_map{$char}})) {
    my $key = $plural_key;
    if ($key =~ /.s$/) {
      if ($key =~ /ies$/) {
        $key =~ s/ies$/y/;
      } else {
        $key =~ s/s$//;
      }
    } elsif ($key =~ /.[^aeiou]ed$/) {
      $key =~ s/ed$//;
    } else {
      next;
    }
    next unless defined $letter_map{$char}{$key};
    my %word_map = %{$letter_map{$char}{$key}};
    for $word (keys(%{$letter_map{$char}{$plural_key}})) {
      $word_map{$word} = 1;
    }
    $letter_map{$char}{$key} = \%word_map;
    delete $letter_map{$char}{$plural_key};
  }
}

# display the current unknown
for my $char (sort keys %letter_map) {
  for $key (sort keys(%{$letter_map{$char}})) {
    my %word_map = %{$letter_map{$char}{$key}};
    my @words = keys(%word_map);
    if (scalar(@words) > 1) {
      print $key." (".(join ", ", sort { length($a) <=> length($b) || $a cmp $b } @words).")";
    } else {
      print $words[0];
    }
    print "\n";
  }
}
