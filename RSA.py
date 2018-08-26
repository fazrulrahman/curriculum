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

def generatePrime():
	rand1 = random.randint(100,1000)
	rand2 = random.randint(100,1000)
	count1 = 0
	count2 = 0
	a = 0
	b = 0

	while count1 != rand1:
		a += 1

		if checkPrime(a):
			count1 += 1

	while count2 != rand2:
		b += 1

		if checkPrime(b):
			count2 += 1

	return a, b

def generatePrimeNumber(a,b):
	while 1:
		rand = random.randint(a,b)
		if checkPrime(rand):
			return rand
 
def gcd(a,b):
 
    if (a == b):
        return a

    if (a > b):
        return gcd(a-b, b)
         
    return gcd(a, b-a)

def lcm(a,b):
    return (a*b) / gcd(a,b)

def egcd(a, b):
    if a == 0:
        return (b, 0, 1)
    else:
        g, y, x = egcd(b % a, a)
        return (g, x - (b // a) * y, y)

def modinv(a, m):
    g, x, y = egcd(a, m)
    if g != 1:
        raise Exception('modular inverse does not exist')
    else:
        return x % m

p, q = generatePrime()
n = p * q
phi = lcm(p-1,q-1) # for calculating Euler's totient function
e = generatePrimeNumber(1,phi)	# co-prime
d = modinv(e,phi)
print p,q,phi,e,d

# n and e are shared

print("n = " + str(n) + "\ne = "+ str(e))

m = 199
c = (m ** e) % n

print c

de = (c ** d) % n

print de

