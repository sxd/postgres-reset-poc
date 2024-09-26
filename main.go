package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/configfile"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres/pool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	v2 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"net/http"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type psqlConnection struct {
	Namespace   string
	Pod         string
	stopChan    chan struct{}
	readyChan   chan struct{}
	Pooler      *pool.ConnectionPool
	PortForward *portforward.PortForwarder
	err         error
}

func (psqlc *psqlConnection) createRequest(interfaceClient *kubernetes.Clientset) *rest.Request {
	return interfaceClient.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Namespace(psqlc.Namespace).
		Name(psqlc.Pod).
		SubResource("portforward")
}

func psqlConnectionNew(namespace, pod string, interfaceClient *kubernetes.Clientset, restClientConfig *rest.Config) (*psqlConnection, error) {
	psqlc := &psqlConnection{}
	if pod == "" {
		return nil, fmt.Errorf("pod not provided")
	}
	psqlc.Namespace = namespace
	psqlc.Pod = pod

	req := psqlc.createRequest(interfaceClient)

	transport, upgrader, err := spdy.RoundTripperFor(restClientConfig)
	if err != nil {
		return nil, err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

	psqlc.readyChan = make(chan struct{}, 1)
	psqlc.stopChan = make(chan struct{})

	psqlc.PortForward, err = portforward.New(
		dialer,
		[]string{"0:5432"},
		psqlc.stopChan,
		psqlc.readyChan,
		os.Stdout,
		os.Stderr,
	)

	return psqlc, err
}

// StartAndWait will begin the forward and wait to be ready
func (psqlc *psqlConnection) StartAndWait() error {
	go func() {
		fmt.Printf("Starting port-forward\n")
		psqlc.err = psqlc.PortForward.ForwardPorts()
		if psqlc.err != nil {
			fmt.Printf("port-forward failed with error %s\n", psqlc.err.Error())
			psqlc.stopChan <- struct{}{}
			return
		}
	}()
	select {
	case <-psqlc.readyChan:
		fmt.Printf("port-forward ready\n")
		return nil
	case <-psqlc.stopChan:
		fmt.Printf("port-forward closed\n")
		return psqlc.err
	}
}

func (psqlc *psqlConnection) createConnectionParameters(user, password string) map[string]string {
	ports, _ := psqlc.PortForward.GetPorts()
	return map[string]string{
		"host":             "localhost",
		"port":             fmt.Sprintf("%d", ports[0].Local),
		"user":             user,
		"password":         password,
		"application_name": "reset-poc",
		"sslmode":          "require",
	}
}

func main() {
	restClientConfig := config.GetConfigOrDie()
	interfaceClient := kubernetes.NewForConfigOrDie(restClientConfig)
	namespace := "postgresql-reset-poc"
	clusterName := "cluster-example"

	clusterSecret, err := interfaceClient.CoreV1().Secrets(namespace).Get(context.Background(), clusterName+"-app", v2.GetOptions{})
	if err != nil {
		panic(err.Error())
	}

	dbname := string(clusterSecret.Data["dbname"])
	dbpass := string(clusterSecret.Data["password"])

	forward, _ := psqlConnectionNew(namespace, "cluster-example-1", interfaceClient, restClientConfig)

	forward.StartAndWait()

	connParameters := forward.createConnectionParameters(dbname, dbpass)

	pooler := pool.NewPostgresqlConnectionPool(configfile.CreateConnectionString(connParameters))

	for i := 0; i < 10; i++ {
		connString := pooler.GetDsn("app")
		conf, err := pgx.ParseConfig(connString)
		if err != nil {
			fmt.Println(err)
		}
		dbConn, err := sql.Open("pgx", stdlib.RegisterConnConfig(conf))
		if err != nil {
			fmt.Printf("error connecting to pgx: %s\n", err)
			continue
		}
		fmt.Printf("Creating queries\n")
		_, err = dbConn.Exec("create table if not exists tests (val int)")
		if err != nil {
			fmt.Printf("error creating table: %s\n", err)
			continue
		}
		_, err = dbConn.Exec("insert into tests values(1)")
		if err != nil {
			fmt.Printf("error inerting data: %s\n", err)
			continue
		}
		fmt.Printf("Closing connection\n")
		dbConn.Close()
	}
	forward.PortForward.Close()
}
