    import pytyrant_async
    from async import to
    pyty = pytyrant_async.Tyrant()
    net = to.TornadoAdaptor(pyty, host, port)
    
    # elsewhere
    from tornado import ioloop
    ioloop.IOLoop.instance().start()
    