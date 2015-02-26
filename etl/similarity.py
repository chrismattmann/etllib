import tika
from tika import parser
import os
import sys
import getopt
import json
tika.initVM()
import operator
from etllib import compareKeySimilarity, compareValueSimilarity, convertKeyUnicode, convertValueUnicode



_verbose = False
_helpMessage = '''

Usage: similarity [-m --metadata] [-r --resemblance] [-v --verbose] [-f directory] [-c file1 file2] [-h --help]


Options:

-m, --metadata
	compare similarity by using file metadata key 

-r, --resemblance
	compare similarity by using file metadata value

-v, --verbose
	Work verbosely rather than silently.

-f, --directory [path to directory]
	read .jpg file from this directory

-c, --file [file1 file2]
	compare similarity of this two files

-h --help
	show help on the screen	
'''

def verboseLog(message):
	if _verbose:
		print >>sys.stderr, message

class _Usage(Exception):
	''' an error for arguments '''

	def __init__(self, msg):
		self.msg = msg

def main(argv = None):
	if argv is None:
		argv = sys.argv

	try:
		try:
			opts, args = getopt.getopt(argv[1:], 'hvf:c:mr', ['help', 'verbose', 'directory=', 'file=' , 'metadata' , 'resemblance'])
		except getopt.error, msg:
			raise _Usage(msg)

		if len(opts) ==0:
			raise _Usage(_helpMessage)

		dirFile = '' 
		first_compare_file = '' 
		second_compare_file = ''
		flag = 1

		for option in argv[1:]:
			if option in ('-m', '--metadata') :
				flag = 1 
			elif option in ('-r', '--resemblance') :
				flag = 2 

		for option, value in opts:
			if option in ('-h', '--help'):
				raise _Usage(_helpMessage)

			elif option in ('-c', '--file'):
				
				#extract file names from command line
				if '-c' in argv :
					index_of_file_option = argv.index('-c')
				else :
					index_of_file_option = argv.index('--file')
				compare_file_name = argv[index_of_file_option+1 : ]

				try : 
					first_compare_file = compare_file_name[0].strip()
					second_compare_file = compare_file_name[1].strip()
				except IndexError, err :
					raise _Usage("You need to input two file names" )

			elif option in ('-f', '--directory'):
				dirFile = value
			elif option in ('-v', '--verbose'):
				global _verbose
				_verbose = True

		file_parsed_data = {}
		filename_list = []
		sorted_resemblance_scores = []

		#count similarity for two given files
		if first_compare_file and second_compare_file :

			first_compare_file_path = os.path.join(dirFile, first_compare_file)
			second_compare_file_path = os.path.join(dirFile, second_compare_file)


			# if file is not in directory
			if not os.path.isfile(first_compare_file_path) :
				raise _Usage("The first file does not exist!")
			elif not os.path.isfile(second_compare_file_path) :
				raise _Usage("The second file does not exist!")
			else:

				filename_list.append(first_compare_file_path)
				filename_list.append(second_compare_file_path)

		#count all files similarity in directory
		else:
			for filename in os.listdir(dirFile):
				if filename.startswith('.'):
					continue

				filename = os.path.join(dirFile, filename)
				
				if not os.path.isfile(filename) :
					continue
				# append all valid filenames
				filename_list.append(filename)

		if flag == 1 :
			sorted_resemblance_scores, file_parsed_data = compareKeySimilarity(filename_list)
			print "Resemblance:\n"

			for tuple in sorted_resemblance_scores:
				print os.path.basename(tuple[0].rstrip(os.sep))+","+str(tuple[1]) + "," + convertKeyUnicode(file_parsed_data[tuple[0]])+'\n'

		elif flag == 2 :
			sorted_resemblance_scores, file_parsed_data = compareValueSimilarity(filename_list)
			print "Resemblance:\n"

			for tuple in sorted_resemblance_scores:
				print os.path.basename(tuple[0].rstrip(os.sep))+","+str(tuple[1]) + "," + convertValueUnicode(file_parsed_data[tuple[0]])+'\n'
			

	except _Usage, err:
		print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
		return 2





if __name__ == "__main__":
	sys.exit(main())





	 