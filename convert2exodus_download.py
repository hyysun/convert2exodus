"""
Insert global variance to a new exodus file locally.


Example:

python convert2exodus_download.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/globalvar/part* \
--template thermal_maze0001.e --output tempglobalvar.e
--tmpdir ./  --varname global_variance  --verbose

"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os
import cProfile
from subprocess import call, check_call

import exopy2 as ep
import numpy as np
from hadoop.typedbytes import TypedBytesWritable
from hadoop.io import SequenceFile

from optparse import OptionParser

def insert_vars(source, destination, varNames, varVals):
    #Copy all dims for new exofile creation
	for d in source.cdf.dimensions.keys():
		if d == 'time_step':
			destination.cdf.createDimension('time_step', source.cdf.dimensions['time_step'])
		elif d == 'num_nod_var':
		    pass
			#destination.cdf.createDimension(d,0)
		else:
			length = source.cdf.dimensions[d]
			destination.cdf.createDimension(d,length)
	
	# put in new variable values
	for (index,value) in enumerate(varVals):
	    name = varNames[index]
        destination.cdf.createVariable(name,('d'),('num_nodes',))
        destination.cdf.variables[name].assignValue(value)
	
	# Copy all variables for new exofile creation
	for var in source.cdf.variables.keys():
		if 'vals_nod_var' in var:
			pass
		elif var == 'name_nod_var':
			pass
		elif var == 'time_whole':
			getvar= source.cdf.variables[var]
			vardata = getvar.getValue()
			var1 = destination.cdf.createVariable(var,(getvar.typecode()),(getvar.dimensions))
			var1.assignValue(vardata)
		elif source.cdf.variables[var].dimensions[0] == 'time_step': # NOTE assume all time dimensions are in first dimension
			continue
		else:
			getvars = source.cdf.variables[var]
			vardata = getvars.getValue()
			thisvar= source.cdf.variables[var]
			vartype  = thisvar.typecode()
			var1 = destination.cdf.createVariable(var,(vartype) ,(thisvar.dimensions))
			var1.assignValue(vardata)	
			attList = dir(thisvar)
			newattlist=[]
			for i in range(len(attList)):
				if attList[i] == 'assignValue':
					pass
				elif attList[i] == 'getValue':
					pass
				elif attList[i] == 'typecode':
					pass
				else:
					newattlist.append(attList[i])
			for a in newattlist:
				attData = getattr(thisvar,a)
				setattr(var1, a, attData)
				
	# copy remaining attributes
	newattlist=[]
	for attr in dir(source.cdf):
		if attr not in dir(destination.cdf):
			newattlist.append(attr)
	for attr in newattlist:
		attData = getattr(source.cdf,attr)
		setattr(destination.cdf, attr, attData)
		
	return 0
    

parser = OptionParser()
parser.add_option('-t', '--template', dest='template',
            help='-t FILE or --template FILE, the template exodus file')
parser.add_option('-o', '--output', dest='output',
            help='-o FILE or --output FILE, the new exodus file')
#use the global tmpdir instead of the current directory for temporary files
parser.add_option('--tmpdir', dest='tmpdir', default = './',
            help='--tmpdir PATH, the temporary directory')
#the name of the variable in the exodus file
parser.add_option('--varname', dest='varname',
            help='--varname NAME, the name of the variable in the exodus file')
parser.add_option('--verbose',
            action="store_true", dest='verbose', default = False,
            help='--verbose, output additional infomation, such as times')

(options, args) = parser.parse_args()

def main():
    inputfiles = sys.argv[1]   
 
    call(['mkdir', os.path.join(options.tmpdir, 'tmp')])
    print "downloading inputfiles  %s"%(inputfiles)
    check_call(['hadoop', 'fs', '-copyToLocal', inputfiles, os.path.join(options.tmpdir, 'tmp')])
    
    order = {}
    values = []

    for fname in os.listdir(os.path.join(options.tmpdir, 'tmp')):
        reader = SequenceFile.Reader(os.path.join(options.tmpdir, 'tmp', fname))
        key_class = reader.getKeyClass()
        value_class = reader.getValueClass()
        key = key_class()
        value = value_class()
        while reader.next(key, value):
            order[int(key.get())] = value.get()
        reader.close()

    var = [ ]  
    for key,val in sorted(order.iteritems()):
        var.extend(val)

    var2 = np.array(var) 

    print "reading templatefile %s"%(options.template)
    templatefile = ep.ExoFile(options.template,'r')
    print "writing outputfile %s"%(options.output)
    newfile = ep.ExoFile(options.output,'w')  

    result = insert_vars(templatefile, newfile, (options.varname,), (var2,))

    print "removing inputfiles  %s"%(inputfiles)
    check_call(['rm', '-r', os.path.join(options.tmpdir, 'tmp')])
    print "Done!"

if __name__=='__main__':
    if options.verbose:
        cProfile.run('main()')
    else:
        main()