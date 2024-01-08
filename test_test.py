import sys
import time

SERVICE = sys.argv[1]
if (SERVICE=="m"):
    STR = "is m"
else:
    STR = "is not m"

if __name__ == '__main__':
    print(STR)