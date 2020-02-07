# -*- coding: utf-8 -*-
"""
SSH object for connecting to a remote machine's shell
"""
from __future__ import (unicode_literals, print_function, division,
                        absolute_import)

import logging
import multiprocessing
import random
import socket
import paramiko
import threading
import re
import time
import select
from qalib.qabase.exceptions import ConnectionError


logging.getLogger('paramiko').setLevel(logging.WARNING)

log = logging.getLogger(__name__)
if not log.handlers:
    log.addHandler(logging.NullHandler())

DEFAULT_SHELL_MATCH_TIMEOUT = 60
INTERACTIVE_SHELL_TIMEOUT = 60
OPEN_SESSION_TIMEOUT = 10

sema = multiprocessing.Semaphore(9)


def _open_session_with_timeout(transport):
    """
    Helper function which encapsulates a difference between older and
    newer Paramiko versions.

    For a given paramiko.Transport object, returns a session opened
    from the tranport, with a timeout of OPEN_SESSION_TIMEOUT seconds.

    New versions have a timeout parameter when opening a session, so
    it's easy.  But on old versions, the timeout is very long and not
    configurable, so it can take us a long time to fail over if we catch
    the node just as it goes down.  So we use a background thread to
    emulate this timeout.
    """
    try:
        # If we're on a new version of paramiko, it's easy
        return transport.open_session(timeout=OPEN_SESSION_TIMEOUT)
    except TypeError:
        pass  # timeout parameter not valid in old versions of paramiko

    # The background thread will write its result into one of these:
    session_list = []
    bg_thread_exception_list = []

    def open_session_in_background():
        """ BG thread which opens a session, stores result in lists above """
        try:
            session_list.append(transport.open_session())
        except Exception as ex:
            bg_thread_exception_list.append(ex)

    # Spawn the background thread
    bg_thread = threading.Thread(target=open_session_in_background)
    bg_thread.name = "QA-OpenParamikoSession-" + bg_thread.name
    bg_thread.daemon = True
    bg_thread.start()
    # Wait for it to complete (or time out):
    bg_thread.join(timeout=OPEN_SESSION_TIMEOUT)
    # Check if it worked:
    if bg_thread_exception_list:
        # the background thread crashed, probably a paramiko exception
        raise bg_thread_exception_list[0]
    elif session_list:
        return session_list[0]  # got it!
    else:
        # The join() call above timed out
        raise ConnectionError("Timed out waiting to establish SSH session")


class SSH(object):

    """
    Simple SSH class used to execute remote commands on a host.

    Bare exec_commands from this object should never be used in tests
    """

    def __init__(self, hostname, username=None, password=None, port=22):
        """
        :param hostname: Hostname of node to connect to
        :type hostname: unicode
        :param username: Username used in SSH authentication
        :type username: unicode
        :param password: Password used in SSH authentication
        :type password: unicode
        :param port: Port used for SSH connection
        :type port: int
        """
        self._hname = hostname
        self._uname = username
        self._passwd = password
        self._port = port
        self.quiet = False
        self.exit_status = None
        self._async = None

    @property
    def hostname(self):
        """
        Hostname configured for this SSH object
        :returns: hostname unicode string
        """
        return self._hname

    @property
    def username(self):
        """
        Username configured for this SSH object
        :returns: username unicode string
        """
        return self._uname

    @property
    def password(self):
        """
        Password configured for this SSH object
        :returns: password unicode string
        """
        return self._passwd

    @property
    def port(self):
        """
        Port configured for this SSH object
        :returns: port integer
        """
        return self._port

    def _open_transport(self):
        """
        Returns a connected paramiko.Transport.
        The caller should close the transport when done with it.
        """
        # There is a paramiko bug where if SSHClient object is not passed to
        # the calling function, it will go out of scope and get GC'd,
        # and its __del__ closes the transport even if it's still in scope.
        # This causes the transport to be closed instantly
        # Paramiko bug: https://github.com/paramiko/paramiko/issues/440
        # For this reason, we just create a Transport directly, and avoid
        # using SSHClient.  If we later decide that we need the convenience
        # of SSHClient, that's fine, but we should be careful about its
        # scope (e.g. by attaching it to whatever we're returning).
        retry_attemps = 100
        for attempt in xrange(retry_attemps):
            try:
                username = self._uname
                password = self._passwd
                hostname = self._hname
                if self._port != 22:
                    hostname = hostname + ":" + str(self._port)
                with sema:
                    transport = paramiko.Transport(hostname)
                    transport.set_keepalive(10)
                    transport.connect(username=username, password=password)
                return transport
            except socket.error:
                log.error("Could not connect: hostname: {} username: {}, port {"
                          "}".format(self._hname, self._uname, self.port))
                raise
            except paramiko.BadAuthenticationType:
                log.error("Bad Authentication: username: {}, password: "
                          "******".format(self._uname))
                raise
            except paramiko.SSHException as ex:
                if "Error reading SSH protocol banner" in str(ex):
                    log.warning("Paramiko will log s stacktrace for this.  "
                                "We will retry this call.")
                    time.sleep(random.uniform(.100, .200))
                else:
                    raise
        msg = "After {} attempts, failed to connect to {}".format(
            retry_attemps, self._hname)
        raise ConnectionError(msg)

    def close(self):
        """ Not currently used """
        pass

    def exec_command(self, cmd, error_expected=False):
        """
        General command to execute remote commands on the configured host.

        :param cmd: The command to execute on the remote host
        :type cmd: unicode
        :param error_expected: Whether a non-zero error code is expected
        from the command execution.
        :type error_expected: bool
        :returns: a tuple containing the exitstatus and output of command
        """
        tp = self._open_transport()
        try:
            # TODO: if we're on a recent version of paramiko, let's use the
            # timeout parameter when opening the session
            session = _open_session_with_timeout(tp)
            session.set_combine_stderr(True)
            session.exec_command(cmd)
            chunks_read = []
            while True:
                data = session.recv(4096)
                if not data:
                    break
                chunks_read.append(unicode(data, "utf-8", "replace"))
            status = session.recv_exit_status()
            if not error_expected and status != 0:
                raise paramiko.SSHException("Non-zero exit status {} from "
                                            "command `{}`".format(status, cmd))
            session.close()
            output = ''.join(chunks_read)
            return status, output
        finally:
            tp.close()

    def exec_command_async(self, cmd):
        """
        General command to execute remote commands on the configured host
        asynchronously. Returns a async shell object to work with.

        :param cmd: The command to execute on the remote host
        :type cmd: unicode
        :returns: ssh.Async object
        """
        tp = self._open_transport()
        return Async(transport=tp, cmd=cmd)

    def file_open(self, filepath, mode):
        """
        Returns a file-like object
        """
        tp = self._open_transport()
        success = False
        try:
            # TODO: timeout
            sftp = paramiko.SFTPClient.from_transport(tp)
            sftpfile = sftp.open(filepath, mode=mode)
            success = True
            return _wrap_close_transport(sftpfile, tp)
        finally:
            if not success:
                tp.close()

    def tcp_open(self, remote_host, remote_port):
        """
        Returns a socket-like object

        Parameters:
          remote_host (str) - remote host to connect to. Eg. 172.28.119.9
          remote_port (int) - remote port to connect to. Eg. 7717
        """
        transport = self._open_transport()
        dest_addr = (unicode(remote_host), remote_port)
        local_addr = ('127.0.0.1', 0)
        # TODO: timeout
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
        channel = _wrap_close_transport(channel, transport)
        return _CloseContextWrapper(channel)


class _CloseContextWrapper(object):

    """ Wraps an object to call its close() on leaving a "with" block """

    def __init__(self, obj):
        """ obj is an object with a close() method """
        self._obj = obj

    def __enter__(self):
        """ Make this a context manager """
        return self._obj

    def __exit__(self, _type, _value, _traceback):
        """ Make this a context manager which calls close() """
        self._obj.close()

    def __getattr__(self, attrname):
        """ Forward attribute lookups to self._obj """
        return getattr(self._obj, attrname)


def _wrap_close_transport(channel, transport):
    """
    Wrap a Channel or SFTPFile object so its close() method also closes
    the underlying Transport.
    """

    def close():
        """ Close this object and the underlying Transport """
        channel._orig_close()  # pylint: disable=protected-access
        transport.close()

    if not hasattr(channel, '_orig_close'):
        setattr(channel, '_orig_close', channel.close)
        setattr(channel, 'close', close)

    return channel


class Async(object):
    """
    Class to allow aysncronous / background execution of commands on
    remote host over SSH
    """

    def __init__(self, transport, cmd):
        """
        :param transport: ssh transport to use for execution, will be closed
                          when done
        :param cmd: the command to run
        """
        self._tp = transport
        self._session = None
        self._output_cache = []
        self._exit_status = None
        self._exit_status_lock = threading.Lock()
        self._thread = None
        self._cmd = cmd
        self._pid = None
        self._last_read_pointer = 0
        self._exec_command(cmd)

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.kill()

    def _exec_command(self, cmd):
        """
        NOTE: Do not call this directly. This method starts a background
        thread to capture output and store it in cache. This command must
        always be accompanied with a follow up kill() command !!

        General command to execute remote commands on the configured host.

        :param cmd: The command to execute on the remote host
        :type cmd: unicode
        """
        if self._thread is not None:
            raise ValueError("Only one command per Async obj")
        if not self._session:
            self._session = _open_session_with_timeout(self._tp)
            self._session.set_combine_stderr(True)

        # Build a shell command-line which prints the PID and executes
        # the caller-supplied command.
        can_use_exec = True
        # don't use "exec" with compound commands:
        if '|' in cmd or ';' in cmd or re.search(r'^\(|[^$]\(', cmd):
            can_use_exec = False
        cmd_word0 = re.split(r'(\s|;|&|\||<|>)', cmd.lstrip())[0]
        # don't try to second-guess if it begins with output redirection:
        if re.match(r'\d*>', cmd) or re.match(r'\d*<', cmd):
            can_use_exec = False
        # don't use "exec" with shell builtins:
        for shell_builtin in ("for", "if", "case", "while", "until",
                              "coproc", "select", "function", "alias",
                              "time", "eval", "exec", "alias", "[", "(", ":"):
            if cmd_word0 == shell_builtin:
                can_use_exec = False
                break
        if can_use_exec:
            fullcmd = "echo $$; exec %s" % cmd
        else:
            fullcmd = "echo $$; %s" % cmd

        self._session.exec_command(fullcmd)
        # get pid
        pid_string = self._recv_pid_line(self._session)
        self._pid = int(pid_string.strip())
        # start output poller
        thread = threading.Thread(target=self._recv_thread)
        thread.daemon = True
        thread.start()
        self._thread = thread

    @staticmethod
    def _recv_pid_line(channel):
        """
        Calls recv() to read exactly one line by recv()ing a byte at a
        time without buffering.
        Performs poorly; not for general use.
        The line is returned; it is not stored.
        """
        buf = ""
        while True:
            data = channel.recv(1)
            if not data:
                break
            if data == "\n" or data == "\r":
                break
            buf += data
        return buf

    def _recv_thread(self):
        """
        Background thread
        Poller to keep reading buffer and put contents into output cache.
        """
        while True:
            data = self._session.recv(65536)
            if not data:
                break  # EOF
            self._output_cache.append([data])
        self._wait_for_exit_status()

    def _wait_for_exit_status(self):
        """
        Waits for the command to exit, returns the exit status.
        NOTE: This could be called by both the main thread (from wait() or
              kill()) or the background thread.
        """
        with self._exit_status_lock:
            if self._exit_status is None:
                self._exit_status = self._session.recv_exit_status()
        return self._exit_status

    def _kill_cmd(self):
        """
        Method to kill the cmd process and all child processes that were
        created
        """
        session = _open_session_with_timeout(self._tp)
        log.debug("Kill process %s", self._pid)
        killcmd = "kill -9 $(pstree " + str(self._pid) + " -p -a -l" + \
                  " | cut -d, -f2 | cut -d' ' -f1)"
        session.exec_command(killcmd)
        session.close()

    def kill(self):
        """
        Kills the cmd run, will also stop output collection once the cmd
        PID is killed.
        """
        if self._thread is None:
            return  # already killed
        self._kill_cmd()
        self._wait_for_exit_status()  # Probably -1
        self._close_and_cleanup()

    def _close_and_cleanup(self):
        """ Called by kill() or wait() """
        if self._thread is not None:
            self._thread.join()  # wait for any remaining output
            self._thread = None
        if self._session is not None:
            self._session.close()
            self._session = None
        if self._tp is not None:
            self._tp.close()
            self._tp = None
        if self._pid is not None:
            self._pid = None

    @property
    def output(self):
        """
        Works similar to file descriptor, returns a block of all output
        """
        output = []
        for block in self._output_cache:
            output.extend(block)
        return ''.join(output)

    @property
    def pid(self):
        """
        Returns the parent pid process
        """
        return self._pid

    def wait(self):
        """
        Waits for the command to exit on its own.
        This is a blocking call

        :return: exitstatus
        """
        self._wait_for_exit_status()
        self._close_and_cleanup()
        return self._exit_status

    def read(self):
        """
        Returns output since last read was performed. An empty string is
        returned if there is no update to cache

        :return: buffer output since last read in string format
        """
        output = ""
        out = []
        if self._output_cache:
            # create a local cache to avoid additions by thread during
            # operation done here.
            local_cache = self._output_cache
            if self._last_read_pointer < len(local_cache):
                for x in local_cache[self._last_read_pointer::]:
                    out.extend(x)
                output = ''.join(out)
                self._last_read_pointer = len(local_cache)
        return output

    @property
    def exitstatus(self):
        """
        This value is returned only if we wait for the process to end on its
        own
        :returns: the exitstatus of the command.
        """
        return self._exit_status


class InteractiveSSH(object):

    def __init__(self, hostname, username=None, password=None, port=22):
        """
        :param hostname: Hostname of node to connect to
        :type hostname: unicode
        :param username: Username used in SSH authentication
        :type username: unicode
        :param password: Password used in SSH authentication
        :type password: unicode
        :param port: Port used for SSH connection
        :type port: int
        """
        self._hname = hostname
        self._uname = username
        self._passwd = password
        self._port = port
        self._client = None
        self.shell = None

    @property
    def connected(self):
        """
        See if SSH session is connected and active
        """
        # Have we connected yet?:
        if self._client is None or self.shell is None:
            return False
        # Does paramiko think it's connected?:
        transport = self.shell.get_transport()
        if transport is None or not transport.is_active():
            return False
        # If we get here, then paramiko thinks it's connected, but
        # let's check for ourselves...
        try:
            transport = self._client.get_transport()
            new_channel = _open_session_with_timeout(transport)
            new_channel.close()
            new_channel = None
        except (socket.error, paramiko.SSHException, ConnectionError):
            # Might as well close these:
            self.shell.close()
            self._client.close()
            return False
        return True  # yes, we are connected

    def connect(self, prompt_regex):
        """
        Connects to the host and log in to shell

        Parameter:
           prompt_regex (str) - regex of prompt expected after ssh login
        """
        if self.connected:
            log.debug("Interactive shell connection already exists, Re-using")
            return
        else:
            log.debug("Interactive shell connection not active, Connecting...")
            self.shell = None
            self._client = None

        with sema:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(self._hname, username=self._uname,
                                 password=self._passwd)
        self.shell = self._client.invoke_shell()
        self.shell.settimeout(INTERACTIVE_SHELL_TIMEOUT)

        self._wait_for_data_and_prompt(prompt_regex)
        log.debug("Interactive shell connection successful!")

    def close(self):
        """
        Closes an interactive shell session
        """
        if self.shell is not None:
            self.shell.close()
        self.shell = None
        if self._client is not None:
            self._client.close()
        self._client = None
        log.info("Interactive shell connection closed!")

    def send_command(self, cmd, prompt_regex,
                     nostrip=False,
                     tolerate_connection_closed=False,
                     timeout=DEFAULT_SHELL_MATCH_TIMEOUT):
        """
        Runs a command on the interactive shell. Returns the output received.

        Parameters:
           cmd (str) - command to be executed
           prompt_regex (str) - regex of prompt expected after command
            execution
           nostrip (bool) - Flag to disable stripping of prompts
           tolerate_connection_closed (bool) - To be used with commands like
                                               'reboot' which cause the
                                               connection to drop
        """
        # connect, will be a NOOP if session exists
        self.connect(prompt_regex)
        if not cmd.endswith("\n"):
            cmd += "\n"
        log.debug("Executing command:%s" % repr(cmd))
        self.shell.send(cmd)
        output = self._wait_for_data_and_prompt(
            prompt_regex,
            timeout=timeout,
            tolerate_connection_closed=tolerate_connection_closed)
        # strip the executed command which is echo'd
        output = re.sub("^" + cmd, "", output)
        if not nostrip:
            output = re.sub(prompt_regex, "", output)
        log.debug("Interactive shell: send_command output:**%s**" % output)
        return output

    def _wait_for_data_and_prompt(self, prompt_regex,
                                  tolerate_connection_closed=False,
                                  timeout=DEFAULT_SHELL_MATCH_TIMEOUT):
        """
        Receives text from buffer and wait for it match regex pattern passed
        """
        if not self.connected:
            raise ConnectionError(
                "Not connected to an interactive shell session")
        output = ""
        start_time = time.time()
        time.sleep(1)
        while True:
            (rlist, _wlist, _xlist) = select.select(
                [self.shell], [], [], 10)
            if len(rlist) > 0:
                data = self.shell.recv(4096)
                # if data is None, that means the channel closed before we
                # get a regex match.
                if not data:
                    if tolerate_connection_closed:
                        break
                    else:
                        # This is an Error condition for this method
                        # as we are trying to match the for a specific regex
                        raise ConnectionError(
                            "Interactive shell session closed"
                            "before matching regex:%s, output:%s" % (
                                prompt_regex, output))
                output += data
                # we match the shell prompt in the data received
                if re.search(prompt_regex, output):
                    break
            if not self.connected:
                if tolerate_connection_closed:
                    break
                else:
                    raise ConnectionError("Connection lost")

            if timeout < (time.time() - start_time):
                if not self.connected:
                    break
                raise ConnectionError(
                    "Interactive shell command timeout reached!"
                    "Regex: %s\nOutput: %s" % (prompt_regex, output))
        return self._cleanup_buffer_output(output)

    def _cleanup_buffer_output(self, data):
        data = self._strip_ansi_sequences(data)
        data = self._cleanup_line_feeds(data)
        return data

    def _cleanup_line_feeds(self, data):
        """
        remove duplicate \\r's in the string that we see
        """
        output = re.sub("\r+", "\r", data)
        output = re.sub("\r\n", "\n", output)
        return output

    def _strip_ansi_sequences(self, data):
        """
        Strips ansi sequences from a string
        """
        ansi_escape = re.compile(r'\x1b\[([0-9]+)?\w')
        return ansi_escape.sub('', data)


class InteractiveSSH(object):

    def __init__(self, hostname, username=None, password=None, port=22):
        """
        :param hostname: Hostname of node to connect to
        :type hostname: unicode
        :param username: Username used in SSH authentication
        :type username: unicode
        :param password: Password used in SSH authentication
        :type password: unicode
        :param port: Port used for SSH connection
        :type port: int
        """
        self._hname = hostname
        self._uname = username
        self._passwd = password
        self._port = port
        self._client = None
        self.shell = None

    @property
    def connected(self):
        """
        See if SSH session is connected and active
        """
        # Have we connected yet?:
        if self._client is None or self.shell is None:
            return False
        # Does paramiko think it's connected?:
        transport = self.shell.get_transport()
        if transport is None or not transport.is_active():
            return False
        # If we get here, then paramiko thinks it's connected, but
        # let's check for ourselves...
        try:
            transport = self._client.get_transport()
            new_channel = _open_session_with_timeout(transport)
            new_channel.close()
            new_channel = None
        except (socket.error, paramiko.SSHException, ConnectionError):
            # Might as well close these:
            self.shell.close()
            self._client.close()
            return False
        return True  # yes, we are connected

    def connect(self, prompt_regex):
        """
        Connects to the host and log in to shell

        Parameter:
           prompt_regex (str) - regex of prompt expected after ssh login
        """
        if self.connected:
            log.debug("Interactive shell connection already exists, Re-using")
            return
        else:
            log.debug("Interactive shell connection not active, Connecting...")
            self.shell = None
            self._client = None

        with sema:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(self._hname, username=self._uname,
                                 password=self._passwd)
        self.shell = self._client.invoke_shell()
        self.shell.settimeout(INTERACTIVE_SHELL_TIMEOUT)

        self._wait_for_data_and_prompt(prompt_regex)
        log.debug("Interactive shell connection successful!")

    def close(self):
        """
        Closes an interactive shell session
        """
        if self.shell is not None:
            self.shell.close()
        self.shell = None
        if self._client is not None:
            self._client.close()
        self._client = None
        log.info("Interactive shell connection closed!")

    def send_command(self, cmd, prompt_regex,
                     nostrip=False,
                     tolerate_connection_closed=False,
                     timeout=DEFAULT_SHELL_MATCH_TIMEOUT):
        """
        Runs a command on the interactive shell. Returns the output received.

        Parameters:
           cmd (str) - command to be executed
           prompt_regex (str) - regex of prompt expected after command
            execution
           nostrip (bool) - Flag to disable stripping of prompts
           tolerate_connection_closed (bool) - To be used with commands like
                                               'reboot' which cause the
                                               connection to drop
        """
        # connect, will be a NOOP if session exists
        self.connect(prompt_regex)
        if not cmd.endswith("\n"):
            cmd += "\n"
        log.debug("Executing command:%s" % repr(cmd))
        self.shell.send(cmd)
        output = self._wait_for_data_and_prompt(
            prompt_regex,
            timeout=timeout,
            tolerate_connection_closed=tolerate_connection_closed)
        # strip the executed command which is echo'd
        output = re.sub("^" + cmd, "", output)
        if not nostrip:
            output = re.sub(prompt_regex, "", output)
        log.debug("Interactive shell: send_command output:**%s**" % output)
        return output

    def _wait_for_data_and_prompt(self, prompt_regex,
                                  tolerate_connection_closed=False,
                                  timeout=DEFAULT_SHELL_MATCH_TIMEOUT):
        """
        Receives text from buffer and wait for it match regex pattern passed
        """
        if not self.connected:
            raise ConnectionError(
                "Not connected to an interactive shell session")
        output = ""
        start_time = time.time()
        time.sleep(1)
        while True:
            (rlist, _wlist, _xlist) = select.select(
                [self.shell], [], [], 10)
            if len(rlist) > 0:
                data = self.shell.recv(4096)
                # if data is None, that means the channel closed before we
                # get a regex match.
                if not data:
                    if tolerate_connection_closed:
                        break
                    else:
                        # This is an Error condition for this method
                        # as we are trying to match the for a specific regex
                        raise ConnectionError(
                            "Interactive shell session closed"
                            "before matching regex:%s, output:%s" % (
                                prompt_regex, output))
                output += data
                # we match the shell prompt in the data received
                if re.search(prompt_regex, output):
                    break
            if not self.connected:
                if tolerate_connection_closed:
                    break
                else:
                    raise ConnectionError("Connection lost")

            if timeout < (time.time() - start_time):
                if not self.connected:
                    break
                raise ConnectionError(
                    "Interactive shell command timeout reached!"
                    "Regex: %s\nOutput: %s" % (prompt_regex, output))
        return self._cleanup_buffer_output(output)

    def _cleanup_buffer_output(self, data):
        data = self._strip_ansi_sequences(data)
        data = self._cleanup_line_feeds(data)
        return data

    def _cleanup_line_feeds(self, data):
        """
        remove duplicate \\r's in the string that we see
        """
        output = re.sub("\r+", "\r", data)
        output = re.sub("\r\n", "\n", output)
        return output

    def _strip_ansi_sequences(self, data):
        """
        Strips ansi sequences from a string
        """
        ansi_escape = re.compile(r'\x1b\[([0-9]+)?\w')
        return ansi_escape.sub('', data)
