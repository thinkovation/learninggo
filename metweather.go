package ukmet
/*
This is an experimental package that calls the UK met office API to get observation data, translating it into a go friendly
structure.
*/
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MetData is the main structure used by the package. It contains a map of LocRecords and
// its own mutex
type MetData struct {
	locations map[string]LocRecord
	apikey    string
	done      chan int
	lock      sync.RWMutex
}

// LocRecord contains the core information about the location as well as a collection of observations
type LocRecord struct {
	Country   string  `json:"country"`
	Continent string  `json:"continent"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Name      string  `json:"name"`
	Idx       string  `json:"i"`
	Elevation float64 `json:"elevation"`
	Data      ObservationCol
}

// ObservationCol is a slice of observations
// It's defined as a type so that the sort helper functions can be
// implemented
type ObservationCol []Observation

// Observation contains the information relating to an observation. Most locations will provide values for all of the phenomena, but some won't
type Observation struct {
	Direction        string  `json:"D"`
	Humidity         float64 `json:"H"`
	Pressure         float64 `json:"P"`
	Speed            float64 `json:"S"`
	Temperature      float64 `json:"T"`
	Visibility       float64 `json:"V"`
	Weather          string  `json:"W"`
	Pressuretendency string  `json:"Pt"`
	Dewpoint         float64 `json:"Dp"`
	//Offsettime       string  `json:"$"`
	TS time.Time
}

// UNEXPORTED types
//
// These types are used internally to marshal the incoming JSON data
// which is then reformatted (and correctly typed) and then put into the public structure - MetData
type metdata struct {
	SiteRep metdatasiterep
}
type metdatasiterep struct {
	Wx metdatawx
	DV metdataDV
}

type metdatawx struct {
	Param []metdataparam
}
type metdataparam struct {
	Name  string `json:"name"`
	Units string `json:"units"`
	Ibble string `json:"$"`
}
type metdataDV struct {
	Datadate string `json:"dataDate"`
	Type     string `json:"type"`
	Location []metdatDVLoc
}
type metdatDVLoc struct {
	Country   string `json:"country"`
	Continent string `json:"continent"`
	Lat       string `json:"lat"`
	Lon       string `json:"lon"`
	Name      string `json:"name"`
	Idx       string `json:"i"`
	Elevation string `json:"elevation"`
	Period    []metdatdvlocperiod
}
type metdatdvlocperiod struct {
	Type    string          `json:"type"`
	Value   string          `json:"value"`
	Reports []metdatareport `json:"Rep"`
}
type metdatareport struct {
	Direction        string `json:"D"`
	Humidity         string `json:"H"`
	Pressure         string `json:"P"`
	Speed            string `json:"S"`
	Temperature      string `json:"T"`
	Visibility       string `json:"V"`
	Weather          string `json:"W"`
	Pressuretendency string `json:"Pt"`
	Dewpoint         string `json:"Dp"`
	Offsettime       string `json:"$"`
}

// init - Not implemented for this package. Trying to avoid x-scoping whenever possible

// Len returns the length of the ObservationCol. Required by the Sort package.
func (ob ObservationCol) Len() int {
	return len(ob)
}

// Swap is used by the sort package
func (ob ObservationCol) Swap(i, j int) {
	ob[i], ob[j] = ob[j], ob[i]
}

// Less is used by the sort package
func (ob ObservationCol) Less(i, j int) bool {
	return ob[i].TS.Before(ob[j].TS)

}

// ObsSort invokes the sort package and sorts the Observations by time in ascending order of TS
func (ob ObservationCol) ObsSort() {
	sort.Sort(ObservationCol(ob))
}

func (md *MetData) GetLocation(locid string) LocRecord {
	var tlr LocRecord
	md.lock.RLock()
	tlr = md.locations[locid]
	md.lock.RUnlock()
	return tlr

}
func (md *MetData) StartSync(minutes int) {
	md.done = make(chan int)
	go md.updateMetData(minutes)

}
func (md *MetData) StopSync() {
	close(md.done)
}

// Load is a concurrency safe function that fetches the latest 24 hours worth of hourly observations, reformats them to a more
// JSON (and user) friendly format, then updates the MetData structure safely by using a mutex.
func (md *MetData) Load(apiKey string) string {

	if apiKey != "" {
		md.apikey = apiKey
	}
	if md.apikey == "" {
		return "Error : No API Key"
	}
	tLocations := make(map[string]LocRecord)
	resp, err := http.Get("http://datapoint.metoffice.gov.uk/public/data/val/wxobs/all/json/all?res=hourly&key=" + md.apikey)
	if err != nil {
		panic(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	// This is not deferred - because we want rid of it as soon as we've got the body
	resp.Body.Close()

	var rs metdata
	err = json.Unmarshal(body, &rs)

	for _, l := range rs.SiteRep.DV.Location {
		var tlr LocRecord
		tlr.Continent = l.Continent
		tlr.Country = l.Country
		televation, _ := strconv.ParseFloat(l.Elevation, 64)
		tlr.Elevation = televation
		tlr.Idx = l.Idx
		tlat, _ := strconv.ParseFloat(l.Lat, 64)
		tlr.Lat = tlat
		tlon, _ := strconv.ParseFloat(l.Lon, 64)

		tlr.Lon = tlon

		tlr.Name = l.Name

		for _, r := range l.Period {
			basetime, _ := time.Parse("2006-01-02Z", r.Value)

			for _, s := range r.Reports {

				var tmpobvs Observation

				mins, terr := strconv.ParseFloat(s.Offsettime, 64)
				if terr == nil {

					tmpobvs.TS = basetime.Add(time.Duration(mins) * time.Minute)
					tmpobvs.Direction = s.Direction
					tmpobvs.Pressuretendency = s.Pressuretendency
					tmpobvs.Weather = s.Weather
					tdp, err := strconv.ParseFloat(s.Dewpoint, 64)
					if err == nil {
						tmpobvs.Dewpoint = tdp
					}
					tspeed, err := strconv.ParseFloat(s.Speed, 64)
					if err == nil {
						tmpobvs.Speed = tspeed
					}
					thum, err := strconv.ParseFloat(s.Humidity, 64)
					if err == nil {
						tmpobvs.Humidity = thum
					}
					tpressure, err := strconv.ParseFloat(s.Pressure, 64)
					if err == nil {
						tmpobvs.Pressure = tpressure
					}
					ttemp, err := strconv.ParseFloat(s.Temperature, 64)
					if err == nil {
						tmpobvs.Temperature = ttemp
					}
					tvis, err := strconv.ParseFloat(s.Visibility, 64)
					if err == nil {
						tmpobvs.Visibility = tvis
					}

					tlr.Data = append(tlr.Data, tmpobvs)
				} else {
					fmt.Println("Ouch")
				}

			}

		}
		tLocations[tlr.Idx] = tlr
	}
	// lock at the last moment
	md.lock.Lock()
	md.locations = tLocations
	for _, v := range md.locations {
		v.Data.ObsSort()
	}
	md.lock.Unlock()
	return "ok"
}

func (md *MetData) updateMetData(interval int) {

	run := true
	for run == true {
		fmt.Println("Invoking Update")
		timer := time.NewTimer(time.Second * time.Duration(interval))
		select {

		case <-md.done:
			// If the done channel is closed then this will trigger, albeit returning nothing (hence our not assigning anything here)
			// see https://dave.cheney.net/2014/03/19/channel-axioms

			run = false
			break

		case <-timer.C:
			break
		}

		if run != true {
			log.Println("Closing syncer")
			break
		}
	}
}
