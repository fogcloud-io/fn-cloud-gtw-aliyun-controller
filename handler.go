package main

import (
	"encoding/base64"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	matcher "github.com/fogcloud-io/routermatcher"
	jsoniter "github.com/json-iterator/go"
)

type CloudGtwDownlinkReq struct {
	FogTopic    string `json:"fog_topic"`
	FogPayload  string `json:"fog_payload"`
	RawClientid string `json:"raw_clientid"`
	RawUsername string `json:"raw_username"`
	RawPassword string `json:"raw_password"`
}

var (
	downlinkMatcher matcher.Matcher

	ErrUnmatchedTopic  = errors.New("unmatched topic")
	ErrInvalidUsername = errors.New("invalid username")
)

func initMatcher() {
	downlinkMatcher = matcher.NewMqttTopicMatcher()

	downlinkMatcher.AddPath(FogTopicThingModelPropSet)
	downlinkMatcher.AddPath(FogTopicThingModelSvcReq)
	downlinkMatcher.AddPath(AliyunTopicThingModelPropSet)
	downlinkMatcher.AddPath(AliyunTopicThingModelSvcReq)
}

func Handler(w http.ResponseWriter, r *http.Request) {
	req := new(CloudGtwDownlinkReq)
	reqBytes, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	jsoniter.Unmarshal(reqBytes, req)

	initMatcher()
	topic, payload, err := HandleDownlink(req.RawClientid, req.RawUsername, req.RawPassword, req.FogTopic, req.FogPayload)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	respBytes, _ := jsoniter.Marshal(struct {
		RawTopic   string `json:"raw_topic"`
		RawPayload string `json:"raw_payload"`
	}{
		RawTopic:   topic,
		RawPayload: payload,
	})
	w.Write(respBytes)
}

const (
	FogTopicThingModelPropSet = "fogcloud/+/+/thing/down/property/set"
	FogTopicThingModelSvcReq  = "fogcloud/+/+/thing/down/service/+"

	AliyunTopicThingModelPropSet = "/sys/+/+/thing/service/property/set"
	AliyunTopicThingModelSvcReq  = "/sys/+/+/thing/service/+"
)

func HandleDownlink(clientid, username, password, fogTopic, fogPayload string) (rawTopic, rawPayload string, err error) {
	log.Printf("fog_topic: %s, fog_payload: %s", fogTopic, fogPayload)

	pk, dn, err := parseUsername(username)
	if err != nil {
		return
	}

	matchedTopic, params, matched := downlinkMatcher.MatchWithAnonymousParams(fogTopic)
	if !matched {
		return "", "", ErrUnmatchedTopic
	}

	switch matchedTopic {
	case FogTopicThingModelPropSet:
		rawTopic = FillTopic(AliyunTopicThingModelPropSet, pk, dn)
		rawPayload = payloadFogToAliyun(fogPayload, "thing.service.property.set")
	case FogTopicThingModelSvcReq:
		if len(params) != 3 {
			return "", "", ErrUnmatchedTopic
		}
		rawTopic = FillTopic(AliyunTopicThingModelSvcReq, pk, dn, params[2])
		rawPayload = payloadFogToAliyun(fogPayload, "thing.service."+params[2])
	}

	return
}

func payloadFogToAliyun(fogPayload string, method string) string {
	fogJson := new(FogJson)
	jsoniter.UnmarshalFromString(fogPayload, fogJson)

	aliJson := new(AliyunJson)
	aliJson.Id = strconv.Itoa(int(fogJson.Id))
	aliJson.Version = "1.0"
	aliJson.Method = method
	aliJson.Params = fogJson.Params

	bytes, _ := jsoniter.Marshal(aliJson)
	return base64.StdEncoding.EncodeToString(bytes)
}

func payloadAliyunToFog(aliyunPayload string, method string) string {
	aliJson := new(AliyunJson)
	jsoniter.UnmarshalFromString(aliyunPayload, aliJson)

	fogJson := new(FogJson)
	fogJson.Version = aliJson.Version
	fogJson.Method = method
	fogJson.Params = aliJson.Params

	bytes, _ := jsoniter.Marshal(aliJson)
	return base64.StdEncoding.EncodeToString(bytes)
}

type FogJson struct {
	Id        uint32                 `json:"id"`
	Version   string                 `json:"version"`
	Method    string                 `json:"method,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	Params    map[string]interface{} `json:"params"`
}

type AliyunJson struct {
	Id      string                 `json:"id"`
	Version string                 `json:"version"`
	Params  map[string]interface{} `json:"params"`
	Method  string                 `json:"method"`
}

func parseUsername(username string) (pk, dn string, err error) {
	params := strings.Split(username, "&")
	if len(params) != 2 {
		err = ErrInvalidUsername
		return
	}

	return params[1], params[0], nil
}

func FillTopic(topic string, replaceStr ...string) string {
	s := topic
	for i := range replaceStr {
		s = strings.Replace(s, "+", replaceStr[i], 1)
	}
	return s
}
