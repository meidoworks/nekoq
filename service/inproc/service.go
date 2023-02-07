package inproc

import "github.com/meidoworks/nekoq/shared/idgen"

type NumGenSpawnApi interface {
	GetNumGen(key string) *idgen.IdGen
}

var numGenSpawn NumGenSpawnApi

func NumGenSpawn(spawn NumGenSpawnApi) NumGenSpawnApi {
	if spawn == nil {
		return numGenSpawn
	} else {
		numGenSpawn = spawn
		return spawn
	}
}
