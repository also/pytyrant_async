import sys
mod_name = sys.argv[1]
cl_name = sys.argv[2]

mod = __import__('async.' + mod_name, fromlist=[mod_name])

cl = getattr(mod, cl_name + 'Adaptor')
import pytyrant
ty = pytyrant.Tyrant()
net = cl(ty, 'localhost', 1978)
ty.get('key', pytyrant.callback)

mod.start()
