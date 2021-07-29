package main

// These are the utilities for stompserver.
//  Authors: Yuyi Guo
// Created: June 2021

import (
	"encoding/json"
	stomp "github.com/go-stomp/stomp"
	lbstomp "github.com/vkuznet/lb-stomp"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

//
// Sitemap  defines maps between the RSE names from the data message and the name Ruci server has.
var Sitemap map[string]string

// DomainRSE: Define the struct of Domain and RSE map.
type DomainRSE struct {
	Domain string
	RSEs   []string
}

//
func inslicestr(s []string, v string) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}
func insliceint(s []int, v int) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}

// parseSitemap is a helper function to parse sitemap.
func parseSitemap(mapFile string) error {
	data, err := ioutil.ReadFile(mapFile)
	if err != nil {
		log.Println("Unable to read sitemap file", err)
		return err
	}
	//log.Println(string(data))
	err = json.Unmarshal(data, &Sitemap)
	if err != nil {
		log.Println("Unable to parse sitemap", err)
		return err
	}
	return nil
}

//findRSEs: For a give domain, find its RSE list
func findRSEs(domain string) []string {
	var RSEs []string
	d := strings.ToUpper(strings.TrimSpace(domain))
	if len(d) == 0 {
		return RSEs
	}
	for _, v := range domainRSEMap {
		vd := strings.ToUpper(strings.TrimSpace(v.Domain))
		if vd == d || strings.Contains(vd, d) || strings.Contains(d, vd) {
			return v.RSEs
		}
	}
	return RSEs
}

// parseRSEMap: for given srver domain to find out the list of RSEs.
func parseRSEMap(RSEfile string) error {
	data, err := ioutil.ReadFile(RSEfile)
	if err != nil {
		log.Println("Unable to read domainRSEMap.txt file", err)
		return err
	}
	err = json.Unmarshal(data, &domainRSEMap)
	if err != nil {
		log.Println("Unable to unmarshal domainRSEMap", err)
		return err
	}
	return nil
}

// subscribe is a helper function to subscribe to StompAMQ end-point as a listener.
func subscribe(endpoint string, stompURI string) (*stomp.Subscription, error) {
	smgr := initStomp(endpoint, stompURI)
	// get connection
	conn, addr, err := smgr.GetConnection()
	if err != nil {
		return nil, err
	}
	log.Println("\n stomp connection", conn, addr)
	// subscribe to ActiveMQ topic
	sub, err := conn.Subscribe(endpoint, stomp.AckAuto)
	if err != nil {
		log.Println("unable to subscribe to", endpoint, err)
		return nil, err
	}
	log.Println("\n stomp subscription", sub)
	return sub, err
}

// initStomp is a function to initialize a stomp object of endpointProducer.
func initStomp(endpoint string, stompURI string) *lbstomp.StompManager {
	p := lbstomp.Config{
		URI:         stompURI,
		Login:       Config.StompLogin,
		Password:    Config.StompPassword,
		Iterations:  Config.StompIterations,
		SendTimeout: Config.StompSendTimeout,
		RecvTimeout: Config.StompRecvTimeout,
		//Endpoint:    Config.EndpointProducer,
		Endpoint:    endpoint,
		ContentType: Config.ContentType,
		Protocol:    Config.Protocol,
		Verbose:     Config.Verbose,
	}
	stompManger := lbstomp.New(p)
	log.Println(stompManger.String())
	log.Println(stompManger.Addresses)
	return stompManger
}

// subscribeTest is a helper function to subscribe to StompAMQ end-point as a listener.
func subscribeTest(endpoint string, stompURI string) (*stomp.Subscription, *stomp.Conn, error) {
	conn, err := stomp.Dial(Config.Protocol, Config.StompURIConsumer,
		stomp.ConnOpt.Login(Config.StompLogin, Config.StompPassword),
		stomp.ConnOpt.HeartBeat(time.Duration(Config.StompSendTimeout)*time.Millisecond, time.Duration(Config.StompRecvTimeout)*time.Millisecond),
		stomp.ConnOpt.HeartBeatGracePeriodMultiplier(float64(Config.Interval)),
	)
	if err != nil {
		log.Printf("Unable to connect to '%s', error %v\n", Config.StompURIConsumer, err)
	} else {
		log.Printf("connected to StompAMQ server '%s'\n", Config.StompURIConsumer)
	}
	// subscribe to ActiveMQ topic
	sub, err := conn.Subscribe(endpoint, stomp.AckAuto)
	if err != nil {
		log.Println("unable to subscribe to", endpoint, err)
		err2 := conn.Disconnect()
		if err2 != nil {
			log.Println("Disconnection error", err2)
		}
		return nil, conn, err
	}
	log.Println("\n stomp subscription", sub)
	return sub, conn, err
}

func safeSubscribe(endpoint string, stompURI string) (*stomp.Subscription, *stomp.Conn) {
	iter := 0
	sub, conn, err := subscribeTest(Config.EndpointConsumer, Config.StompURIConsumer)
	if err != nil {
		for iter = 1; iter <= Config.MaxSubTrial; iter++ {
			log.Println("Subscription unsuccessful: ", err)
			log.Printf("Could not subscribe, will try in %d seconds\n", time.Duration(Config.Interval)*time.Millisecond)
			time.Sleep(time.Duration(Config.Interval) * time.Millisecond)
			sub, conn, err = subscribeTest(Config.EndpointConsumer, Config.StompURIConsumer)
			if err == nil {
				break
			}
		}
		if err != nil {
			err2 := conn.Disconnect()
			if err2 != nil {
				log.Println("Disconnection error", err2)
			}
			log.Printf("Could not subscribe even until max trials:", Config.MaxSubTrial)
			log.Printf("Exit!")
			if err != nil {
				os.Exit(1)
			}
		}
	}
	log.Println("Successful subscription, trial:", iter)
	return sub, conn
}

func safeUnsubscribe(sub *stomp.Subscription, conn *stomp.Conn) {
	if err := sub.Unsubscribe(); err != nil {
		log.Println("Unsubscribe error:", err)
	}
	log.Println("Unsubscribed successfully.")
	if err := conn.Disconnect(); err != nil {
		log.Println("Disconnection error:", err)
	}
	log.Println("Disconnected successfully.")
}
