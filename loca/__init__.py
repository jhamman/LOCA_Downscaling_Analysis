
def print_date():
    '''Helper function to print current licencse and time'''
    from datetime import datetime
    import getpass
    import socket

    print('Last executed: %s by %s on %s' % (datetime.now(), getpass.getuser(),
                                             socket.gethostname()), flush=True)
