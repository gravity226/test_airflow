import random
import time

from pyspark import SparkContext

print('started pi job')

time.sleep(900)

NUM_SAMPLES = 1000

def inside(p):
  x, y = random.random(), random.random()
  return x*x + y*y < 1

sc = SparkContext()
count = sc.parallelize(c=range(0, NUM_SAMPLES)).filter(inside).count()
pi = (4.0 * count / NUM_SAMPLES)
print ("Pi is roughly %f" % pi)
