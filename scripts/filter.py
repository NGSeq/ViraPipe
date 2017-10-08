def finder(fasta, blast):
	for f in fasta:
		found = False
		fs = f.split(" ")
                fk = fs[0].replace(">",'')
		for b in blast:	
			bs = b.split('\t')	
			if bs[0] == fk:
				found = True
				overlap = ((int(bs[7])-(int(bs[6])+1))/len(f[1]))*100
				if overlap>70 and float(bs[2])>70:
					print fs[0]+"\n"+fs[1].rstrip() #keep the fasta sequence if overlap and pident values are over threshold
		if found==False:
			print fs[0]+"\n"+fs[1].rstrip()  #keep non-human
					 

import sys

fasta = open(sys.argv[1],'rb')
blast = open(sys.argv[2],'rb')
finder(fasta,blast)

