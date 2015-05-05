package jolt

import (
	"fmt"
	"strconv"
	"github.com/boltdb/bolt"
	"net/http"
	"bytes"
	"encoding/json"
)

type KeyValue struct {
	Key string
	Value string
	BucketName []string
}

var DB *bolt.DB

func Start(addr string) {
	http.Handle("/", http.FileServer(assetFS()))
	http.HandleFunc("/bucket/list", bucketList) // name[] = array with bucket path , skip, limit
	http.HandleFunc("/bucket/scan/prefix", prefixScan) // name[] = array with bucket path , prefix , skip, limit
	http.ListenAndServe(addr, nil)
}

/*
/*  Routes
*/

func bucketList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var list []KeyValue
	var err error
	var limit int
	var skip int

	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil {
		limit = 100
	}

	if skip, err = strconv.Atoi(r.URL.Query().Get("skip")); err != nil {
		skip = 0
	}

	nameList := r.URL.Query()["name[]"]
	if nameList == nil {
		list, err = listRoot(skip, limit)
		if err != nil {
			http.Error(w, "{\"error\":\"" + err.Error() + "\"}", http.StatusInternalServerError)
		}
	} else {
		list, err = listBucket(nameList, skip, limit)
		if err != nil {
			http.Error(w, "{\"error\":\"" + err.Error() + "\"}", http.StatusInternalServerError)
		}
	}

	json.NewEncoder(w).Encode(list)
}

func prefixScan(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var list []KeyValue
	var err error
	var limit int
	var skip int
	var prefix string = r.URL.Query().Get("prefix")

	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil {
		limit = 100
	}

	if skip, err = strconv.Atoi(r.URL.Query().Get("skip")); err != nil {
		skip = 0
	}

	nameList := r.URL.Query()["name[]"]
	if nameList != nil {
		list, err = prefixScanBucket(nameList, []byte(prefix), skip, limit)
		if err != nil {
			http.Error(w, "{\"error\":\"" + err.Error() + "\"}", http.StatusInternalServerError)
		}
	} else {
		http.Error(w, "{\"error\":\"" + err.Error() + "\"}", http.StatusInternalServerError)
	}

	json.NewEncoder(w).Encode(list)
}

/*
/*  Actions
*/

func listBucket(nameSlice []string, skip int, limit int) ( kvs []KeyValue, err error) {
	err = DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(nameSlice[0]))
		if b == nil {
			return fmt.Errorf("Bucket %q not found", nameSlice[0])
		}

		//get bucket from name list
		for _,name := range nameSlice[1:] {
			b = b.Bucket([]byte(name))
			if b == nil {
				return fmt.Errorf("Bucket %q not found", name)
			}
		}

		c := b.Cursor()

		count := 0
		for k, v := c.First(); k != nil && (count < limit || limit == 0); k, v = c.Next() {
			if count < skip {
				count++
				continue
			}
			kv := KeyValue{Key:string(k), Value:string(v)}
			if v == nil {
				if bkt := b.Bucket(k); bkt != nil {
					kv.BucketName = append(nameSlice, string(k))
				}
			}

			kvs = append(kvs, kv)
			count++
		}

		return nil
	})

	return kvs, err
}

func listRoot(skip int, limit int) ([]KeyValue, error) {
	var buckets []KeyValue
	err := DB.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()

		count := 0
		for k, v := c.First(); k != nil && (count < limit || limit == 0); k, v = c.Next() {
			if count < skip {
				count++
				continue
			}
			kv := KeyValue{Key:string(k), Value:string(v)}
			if v == nil {
				if bkt := tx.Bucket(k); bkt != nil {
					kv.BucketName = []string{string(k)}
				}
			}
			buckets = append(buckets, kv)
			count++
		}
		return nil
	})
	return buckets, err
}

func prefixScanBucket(nameSlice []string, prefix []byte, skip int, limit int) ( kvs []KeyValue, err error) {
	err = DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(nameSlice[0]))
		if b == nil {
			return fmt.Errorf("Bucket %q not found", nameSlice[0])
		}

		//get bucket from name list
		for _,name := range nameSlice[1:] {
			b = b.Bucket([]byte(name))
			if b == nil {
				return fmt.Errorf("Bucket %q not found", name)
			}
		}

		c := b.Cursor()

		count := 0
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix) && (count < limit || limit == 0); k, v = c.Next() {
			if count < skip {
				count++
				continue
			}
			kv := KeyValue{Key:string(k), Value:string(v)}
			if v == nil {
				if bkt := b.Bucket(k); bkt != nil {
					kv.BucketName = append(nameSlice, string(k))
				}
			}

			kvs = append(kvs, kv)
			count++
		}

		return nil
	})

	return kvs, err
}
