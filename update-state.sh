#!/bin/bash
Q='"'
q="'"
strip_lead() {
  perl -ne 's/^\s+(\S)/$1/; print'
}
strip_blanks() {
  perl -ne 'next unless /./; print'
}
strip_lead_and_blanks() {
  strip_lead | strip_blanks
}
path_to_pattern() {
  perl -pne 's/^/^/;s/\./\\./g;s/$/\$/'
}
generate_instructions() {
  instructions=$(mktemp)
  if [ -z "$skip_wrapping" ]; then
    echo '(cd $(git rev-parse --show-toplevel)' >> $instructions
    to_retrieve_expect >> $instructions
  fi
  if [ -n "$patch_remove" ]; then
    if [ -z "$expect_files" ]; then
      expect_files=$expect_file
    fi
    echo 'perl -e '$q'
      my @expect_files=qw('$q$Q"$expect_files"$Q$q');
      @ARGV=@expect_files;
      my @stale=qw('$q$Q"$patch_remove"$Q$q');
      my $re=join "|", @stale;
      my $suffix=".".time();
      my $previous="";
      sub maybe_unlink { unlink($_[0]) if $_[0]; }
      while (<>) {
        if ($ARGV ne $old_argv) { maybe_unlink($previous); $previous="$ARGV$suffix"; rename($ARGV, $previous); open(ARGV_OUT, ">$ARGV"); select(ARGV_OUT); $old_argv = $ARGV; }
        next if /^(?:$re)(?:(?:\r|\n)*$| .*)/; print;
      }; maybe_unlink($previous);'$q |
    strip_lead >> $instructions
  fi
  if [ -n "$patch_add" ]; then
    echo 'perl -e '$q'
      my $new_expect_file="'$new_expect_file'";
      use File::Path qw(make_path);
      use File::Basename qw(dirname);
      make_path (dirname($new_expect_file));
      open FILE, q{<}, $new_expect_file; chomp(my @words = <FILE>); close FILE;
      my @add=qw('$q$Q"$patch_add"$Q$q');
      my %items; @items{@words} = @words x (1); @items{@add} = @add x (1);
      @words = sort {lc($a)."-".$a cmp lc($b)."-".$b} keys %items;
      open FILE, q{>}, $new_expect_file; for my $word (@words) { print FILE "$word\n" if $word =~ /\w/; };
      close FILE;
      system("git", "add", $new_expect_file);
    '$q |
    strip_lead >> $instructions
  fi
  if [ -n "$should_exclude_patterns" ]; then
    echo "(cat $q$excludes_file$q - <<EOF
    $should_exclude_patterns
    EOF
    ) |grep .|
    sort -f |
    uniq > $q$excludes_file.temp$q &&
    mv $q$excludes_file.temp$q $q$excludes_file$q" | strip_lead >> $instructions
  fi
  if [ -z "$skip_wrapping" ]; then
    echo ')' >> $instructions
  fi
  echo $instructions
}
patch_variables() {
  if [ -n "$patch_remove" ]; then
    echo '
      patch_remove=$(perl -ne '$q'next unless s{^</summary>(.*)</details>$}{$1}; print'$q' < '$1')
      ' | strip_lead
  fi
  if [ -n "$patch_add" ]; then
    echo '
      patch_add=$(perl -e '$q'$/=undef;
        $_=<>;
        s{<details>.*}{}s;
        s{^#.*}{};
        s{\n##.*}{};
        s{(?:^|\n)\s*\*}{}g;
        s{\s+}{ }g;
        print'$q' < '$1')
      ' | strip_lead
  fi
  if [ -n "$should_exclude_patterns" ]; then
    echo '
      should_exclude_patterns=$(perl -e '$q'$/=undef;
        $_=<>;
        exit unless s{(?:You should consider excluding directory paths|You should consider adding them to).*}{}s;
        s{.*These sample patterns would exclude them:}{}s;
        s{.*\`\`\`([^`]*)\`\`\`.*}{$1}m;
        print'$q' < '$1' | grep . || true)
    ' | strip_lead
  fi
}
