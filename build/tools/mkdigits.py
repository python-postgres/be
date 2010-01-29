import sys
sys.dont_write_bytecode = True
import itertools

# generate comma separated ordinals from the characters in the file.
rewritten_lines = (",".join(map(lambda y: str(ord(y)),x))+"," for x in sys.stdin)

# write to stdout
sys.stdout.writelines(itertools.chain(rewritten_lines, "0", "\n"))

# done
sys.exit(0)
