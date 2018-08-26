import random
def checkPrime(num):
	flag = 1
	if num % 2 == 0:
		return 0
	if num % 3 == 0:
		return 0
	if num % 5 == 0:
		return 0

	y = 6
	while (y * y <= num):
		if(num % (y-1) == 0 or num % (y+1) == 0):
			return 0
		y = y + 6
		
	return 1

def generatePrime(c,d):
	rand1 = random.randint(c,d)
	count = 0
	a = 0

	while count != rand1:
		a += 1
		if checkPrime(a):
			count += 1

	return a

g = generatePrime(100,1000)
n = generatePrime(10,50)
print g,n

a = random.randint(1,n)
b = random.randint(1,n)
print a,b

Ga = (g ** a) % n
Gb = (g ** b) % n
print Ga,Gb

secreta = (Gb ** a) % n
secretb = (Ga ** b) % n

print secreta, secretb
