
import copy

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class Bridge(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._cfg     = copy.deepcopy(cfg)

        self._channel = self._cfg['name']
        self._owner   = self._cfg['owner']
        self._logdir  = self._cfg['logdir']
        self._uid     = self._cfg['uid']
        self._pwd     = self._cfg['pwd']

        if self._owner:
            self._uid = '%s.%s' % (self._owner, self._uid)

        self._log     = ru.Logger(name=self._uid,
                                  path=self._logdir, 
                                  level=self._cfg.get('log_level'))
        self._log.debug('=== debug')

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10:  # logging.DEBUG:
            self._debug  = True
        else:
            self._debug  = False


    # --------------------------------------------------------------------------
    #
    def start(self):
        pass


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(cfg):

        # ----------------------------------------------------------------------
        # NOTE:  I'd rather have this as class data than as stack data, but
        #        python stumbles over circular imports at that point :/
        #        Another option though is to discover and dynamically load
        #        components.
        from .pubsub import Pubsub
        from .queue  import Queue

        _btypemap = {'pubsub' : Pubsub, 
                     'queue'  : Queue}
        # ----------------------------------------------------------------------

        sid  = cfg['sid']
        kind = cfg['kind']
        uid  = cfg['uid']

        from .. import session as rp_session
        session = rp_session.Session(uid=sid, _cfg=cfg)
        log     = ru.Logger(uid, level='DEBUG')  # FIXME level
        log.debug('start bridge %s [%s]', uid, kind)

        if kind not in _btypemap:
            raise ValueError('unknown bridge type (%s)' % kind)

        btype  = _btypemap[kind]
        bridge = btype(cfg, session)

        return bridge


# ------------------------------------------------------------------------------

