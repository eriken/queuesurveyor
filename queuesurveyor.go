package queuesurveyor

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/eriken/queuesurveyor/redis"
)

/*
	entries that fail need to be retried;
	the logic here is to fetch all the current processed entries in
	JPAPI_GEOADDRESS_ALBUM_CHECK_IN_PROGRESS every process_list_clean_interval
	minutes. Then, every process_list_clean_interval minutes
	check if the entry is more than PROCESS_LIST_RETRY_TRESHHOLD minutes old.
	If so check if it still is in JPAPI_GEOADDRESS_ALBUM_CHECK_IN_PROGRESS, if not
	delete it, else put it back into the list it came from.
*/

var (
	redisClient redis.Redis
)

type QueueSurveyor struct {
	archive map[string]Item
	current map[string]Item
	sleep   int
	ttl     int
}

type Item struct {
	added    time.Time
	checksum string
	payload  string
}

func Init(
	addr string,
	db int,
	masterList,
	progressList string,
	sleep,
	ttl int,
) {
	new(
		sleep,
		ttl,
		redis.Init(
			addr,
			db,
			masterList,
			progressList,
		),
	).survey()
}

func IntiWithSentinel(
	masterName,
	sentinelAddr string,
	db int,
	masterList,
	progressList string,
	sleep,
	ttl int,
) {
	new(
		sleep,
		ttl,
		redis.InitWithSentinel(
			masterName,
			sentinelAddr,
			db,
			masterList,
			progressList,
		),
	).survey()
}

func new(
	sleep,
	ttl int,
	redisClientParam redis.Redis,
) QueueSurveyor {
	redisClient = redisClientParam
	return QueueSurveyor{
		archive: make(map[string]Item),
		current: make(map[string]Item),
		sleep:   sleep,
		ttl:     ttl,
	}
}

func Close() {
	redisClient.Close()
}

func (queueSurveyor QueueSurveyor) survey() {
	for {
		time.Sleep(time.Minute * time.Duration(queueSurveyor.sleep))

		queueSurveyor.current = make(map[string]Item) //reset

		err := queueSurveyor.poll()
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(time.Second * 1)
			continue
		}

		queueSurveyor.merge()

		counter, err := queueSurveyor.purge()
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(time.Second * 1)
			continue
		}

		if counter > 0 {
			fmt.Printf(
				"pushed %d lagging progress entrie(s) back into master list, sleeping %d minutes\n",
				counter,
				queueSurveyor.sleep,
			)
		} else {
			fmt.Printf(
				"no entries to push back to master list, sleeping %d minutes\n",
				queueSurveyor.sleep,
			)
		}
	}
}

func (queueSurveyor *QueueSurveyor) poll() error {

	entries, err := redisClient.Range()
	if err != nil {
		return fmt.Errorf(
			"unable to execute queue range query, reason: %s",
			err.Error(),
		)
	}

	for _, entry := range entries {
		checksum := fmt.Sprintf(
			"%x",
			md5.Sum([]byte(entry)),
		)
		item := Item{
			added:    time.Now(),
			checksum: checksum,
			payload:  entry,
		}

		queueSurveyor.current[checksum] = item
	}

	return nil
}

func (queueSurveyor *QueueSurveyor) merge() {
	for _, item := range queueSurveyor.current {
		_, found := queueSurveyor.archive[item.checksum]
		if !found {
			queueSurveyor.archive[item.checksum] = item
		}
	}

	for _, archiveItem := range queueSurveyor.archive {
		_, found := queueSurveyor.current[archiveItem.checksum]
		if !found {
			delete(queueSurveyor.archive, archiveItem.checksum)
		}
	}
}

func (queueSurveyor *QueueSurveyor) purge() (int, error) {
	counter := 0
	for _, item := range queueSurveyor.archive {
		if time.Now().After(item.added.Add(time.Minute * time.Duration(queueSurveyor.ttl))) {
			delete(queueSurveyor.archive, item.checksum)

			err := queueSurveyor.pushback(item)
			if err != nil {
				return 0, err
			}

			counter++
		}
	}
	return counter, nil
}

func (queueSurveyor QueueSurveyor) pushback(item Item) error {
	err := redisClient.Push(item.payload)
	if err != nil {
		return fmt.Errorf(
			"unable to push file back to queue, reason: %s",
			err.Error(),
		)
	}
	err = redisClient.Remove(item.payload)
	if err != nil {
		return fmt.Errorf(
			"unable to delete file from queue progress list, reason: %s",
			err.Error(),
		)

	}
	return nil
}
