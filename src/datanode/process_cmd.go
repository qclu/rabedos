package datanodeIdc

import (
	"utils/rbdlogger"
)

func preHandleCmds(taskCmdCh, loadVolCmdCh, replicationFileCmdCh chan *Cmd, cmds []*Cmd) (preResps []*CmdResp) {
	var cmdCh chan *Cmd
	preResps = make([]*CmdResp, len(cmds))
	for i, c := range cmds {
		preResps[i] = &CmdResp{Result: CmdRunning, Id: c.Id}
		if c.Code == OpVolSnapshot {
			cmdCh = loadVolCmdCh
		} else if c.Code == OpReplicationFile {
			cmdCh = replicationFileCmdCh
		} else {
			cmdCh = taskCmdCh
		}
		cmdCh <- c
	}
	log.GetLog().LogDebug("preHandleCmds preResps:", preResps)

	return
}

func (s *DataNode) handleCmds(taskCmdCh, loadVolCmdCh, replicationFileCmdCh chan *Cmd, respCh chan *CmdResp) {
	handleCmds := func(cmdCh chan *Cmd, respCh chan *CmdResp) {
		for {
			select {
			case cmd := <-cmdCh:
				resp := s.processCmd(cmd)
				respCh <- resp
			}
		}
		ump.Alarm(UmpKeyCmds, UmpDetailCmds)
	}

	go handleCmds(loadVolCmdCh, respCh)
	go handleCmds(replicationFileCmdCh, respCh)
	handleCmds(taskCmdCh, respCh)
}

func rmRepeat(resp *CmdResp, cmdResps []*CmdResp) (replace bool) {
	for i, r := range cmdResps {
		if r.Id == resp.Id {
			cmdResps[i] = resp
			replace = true
			break
		}
	}

	return
}

func getBatchResps(hb *Hb, respCh chan *CmdResp) {
	for {
		select {
		case resp := <-respCh:
			//if !rmRepeat(resp, hb.TaskResps) {
			hb.TaskResps = append(hb.TaskResps, resp)
			//}
		default:
			return
		}
	}
}
