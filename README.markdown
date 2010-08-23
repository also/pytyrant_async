    import pytyrant_async
    from async import tornado_adaptor
    pyty = pytyrant_async.Tyrant()
    net = tornado_adaptor.TornadoAdaptor(pyty, host, port)
    
    # elsewhere
    from tornado import ioloop
    ioloop.IOLoop.instance().start()
    