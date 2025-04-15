#!/usr/bin/env python3

import sys
import re

for l in sys.stdin:
    l = l.strip()
    if not l:
        continue

    p = l.split("\t", 2)
    if len(p) < 3:
        continue

    i = int(p[0])
    t = f"{p[1]} {p[2]}"
    toks = [w for w in re.split(r"\W+", t.lower()) if w]

    for tok in toks:
        print(f"{tok}\t{i}\t1")

    print(f"@@TIT\t{i}\t{p[1]}")
    print(f"@@LEN\t{i}\t{len(toks)}")
    print(f"@@ID\t{i}\t1")
