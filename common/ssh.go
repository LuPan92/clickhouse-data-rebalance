package common

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/LuPan92/clickhouse-data-rebalance/log"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

const (
	TmpWorkDirectory string = "/tmp"
)

type SshOptions struct {
	User             string
	Password         string
	Host             string
	Port             int
	NeedSudo         bool
	AuthenticateType int
}

func sshConnectwithPassword(user, password string) (*ssh.ClientConfig, error) {
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func SSHConnect(opts SshOptions) (*ssh.Client, error) {
	var (
		addr         string
		clientConfig *ssh.ClientConfig
		client       *ssh.Client
		err          error
	)

	if opts.AuthenticateType == SshPasswordSave {
		clientConfig, err = sshConnectwithPassword(opts.User, opts.Password)
	} else {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	// connet to ssh
	addr = net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port))
	if client, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	return client, nil
}

func SSHRun(client *ssh.Client, password, shell string) (result string, err error) {
	var session *ssh.Session
	var buf []byte
	// create session
	if session, err = client.NewSession(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer session.Close()

	log.Logger.Debugf("shell: %s", shell)

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,     // disable echoing
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}
	err = session.RequestPty("xterm", 80, 40, modes)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	in, err := session.StdinPipe()
	if err != nil {
		return "", errors.Wrap(err, "")
	}

	out, err := session.StdoutPipe()
	if err != nil {
		return "", errors.Wrap(err, "")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(in io.WriteCloser, out io.Reader, output *[]byte) {
		defer wg.Done()
		var (
			line string
			r    = bufio.NewReader(out)
		)

		for {
			b, err := r.ReadByte()
			if err != nil {
				break
			}
			*output = append(*output, b)
			if b == byte('\n') {
				line = ""
				continue
			}
			line += string(b)
			// TODO I have no idea to slove this problem: "xxx is not in the sudoers file.  This incident will be reported."
			if strings.HasPrefix(line, "[sudo] password for ") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte(password + "\n"))
				if err != nil {
					break
				}
			}
		}
	}(in, out, &buf)

	_, err = session.CombinedOutput(shell)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	wg.Wait()
	result = strings.TrimSpace(string(buf))
	result = result[strings.Index(result, "i love china")+12:]
	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "[sudo] password for ") {
		result = result[strings.Index(result, "\n")+1:]
	}
	log.Logger.Debugf("output:[%s]", result)
	return
}

func RemoteExecute(opts SshOptions, cmd string) (string, error) {
	client, err := SSHConnect(opts)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("host: %s, cmd: %s", opts.Host, cmd))
	}
	defer client.Close()

	finalScript := genFinalScript(opts.User, cmd, opts.NeedSudo)
	var output string
	if output, err = SSHRun(client, opts.Password, finalScript); err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("run '%s' on host %s fail: %s", cmd, opts.Host, output))
	}
	return output, nil
}

func genFinalScript(user, cmd string, needsudo bool) string {
	var shell string
	if user != "root" && needsudo {
		cmds := strings.Split(cmd, ";")
		for index, command := range cmds {
			cmds[index] = fmt.Sprintf("sudo %s", command)
		}
		cmd = strings.Join(cmds, ";")

		/* if LANG=zh_CN.UTF-8, maybe print message like this:
		我们信任您已经从系统管理员那里了解了日常注意事项。
		总结起来无外乎这三点：

		    #1) 尊重别人的隐私。
		    #2) 输入前要先考虑(后果和风险)。
		    #3) 权力越大，责任越大。

		[sudo] username 的密码：
		so we need convert charset first.
		*/

		shell = fmt.Sprintf("export LANG=en_US.UTF-8; %s", cmd)
	} else {
		shell = cmd
	}
	shell = fmt.Sprintf("echo 'i love china'; %s", shell)
	return shell
}
