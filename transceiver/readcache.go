package transceiver

import (
	"github.com/tgres/tgres/rrd"
	"time"
)

type ReadCache struct {
	serde rrd.SerDe
	dsns  *rrd.DataSourceNames
}

func (r *ReadCache) Reload() error {
	return r.dsns.Reload(r.serde)
}

// Satisfy DSGetter interface in tgres/dsl

func (r *ReadCache) GetDSById(id int64) *rrd.DataSource {
	ds, _ := r.serde.FetchDataSource(id)
	return ds
}

func (r *ReadCache) DsIdsFromIdent(ident string) map[string]int64 {
	return r.dsns.DsIdsFromIdent(ident)
}

func (r *ReadCache) SeriesQuery(ds *rrd.DataSource, from, to time.Time, maxPoints int64) (rrd.Series, error) {
	return r.serde.SeriesQuery(ds, from, to, maxPoints)
}

// End of DSGetter

func (r *ReadCache) FsFind(pattern string) []*rrd.FsFindNode {
	r.dsns.Reload(r.serde)
	return r.dsns.FsFind(pattern)
}
