import itertools
import sys

# generate comma separated ordinals from the characters in the file.
rewritten_lines = (",".join(map(lambda y: str(ord(y)),x))+"," for x in sys.stdin)

# write to stdout
sys.stdout.writelines(itertools.chain(rewritten_lines, "0", "\n"))

# done
sys.exit(0)
