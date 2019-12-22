# I was too lazy to use actix web for tests
# This is way easier

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m'

FAILED="${RED}FAILED${NC}:"
PASSED="${GREEN}PASSED${NC}:"

function create_order {
    redis-cli -p 6380 HSET good_id:1 count 5

    status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        'localhost:8080/user/1/order' -d '{"goods": [{"id": 1, "count": 1}]}')

    if [[ $status_code -ne 201 ]] ; then
        echo -e "$FAILED expected 201 was $status_code"
    else
        echo -e "$PASSED /user/1/order POST"
    fi
}

function update_order_op_update {
    status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X PUT 'localhost:8080/user/1/order/1' \
        -d '{"goods": [{"id": 1, "count": 3, "operation": "update"}]}')

    if [[ $status_code -ne 200 ]] ; then
        echo -e "$FAILED expected 200 was $status_code"
    else
        echo -e "$PASSED /user/1/order/1 PUT"
    fi
}

function update_order_op_delete {
    status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X PUT 'localhost:8080/user/1/order/1' \
        -d '{"goods": [{"id": 1, "count": 1, "operation": "delete"}]}')

    if [[ $status_code -ne 200 ]] ; then
        echo -e "$FAILED expected 200 was $status_code"
    else
        echo -e "$PASSED /user/1/order/1 PUT"
    fi
}

function get_order {
    response=($(curl -s -w "\n%{http_code}" 'localhost:8080/user/1/order/1' | {             
        read body
        read code
        echo $code
        echo $body
    }))

    if [[ ${response[0]} -ne 200 || "${response[1]}" != "$1" ]] ; then
        echo -e "$FAILED expected 200 was ${response[0]}"
        echo -e "$FAILED expected $1 was ${response[1]}"
    else
        echo -e "$PASSED /user/1/order/1 GET"
    fi
}

function delete_order {
    status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X DELETE 'localhost:8080/user/1/order/1')

    if [[ $status_code -ne 200 ]] ; then
        echo -e "$FAILED expected 200 was $status_code"
    else
        echo -e "$PASSED /user/1/order/1 DELETE"
    fi

    redis-cli -p 6379 flushdb
    redis-cli -p 6380 flushdb
}

function create_billing {
    status_code=$(curl -s -o /dev/null -w "%{http_code}" \
        'localhost:8080/user/1/order/1/billing' -d '{"id": 1}')

    if [[ $status_code -ne 201 ]] ; then
        echo -e "$FAILED expected 201 was $status_code"
    else
        echo -e "$PASSED /user/1/order/1/billing POST"
    fi
}

function test_create_get_delete_order {
    create_order
    sleep 0.1
    get_order '{"status":"new","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    delete_order
    sleep 0.1
}

function test_create_update_get_delete_order {
    create_order
    sleep 0.1
    update_order_op_update
    sleep 0.1
    get_order '{"status":"new","goods":[{"id":1,"count":3,"naming":""}]}'
    sleep 0.1
    delete_order
    sleep 0.1

    create_order
    sleep 0.1
    update_order_op_delete
    sleep 0.1
    get_order '{"status":"new","goods":[]}'
    sleep 0.1
    delete_order
    sleep 0.1
}

function test_billing {
    create_order
    sleep 0.1
    get_order '{"status":"new","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    create_billing
    sleep 0.1
    get_order '{"status":"payed","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    delete_order
    sleep 0.1
}

function test_update_after_billing {
    create_order
    sleep 0.1
    get_order '{"status":"new","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    create_billing
    sleep 0.1
    get_order '{"status":"payed","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    update_order_op_update
    sleep 0.1
    get_order '{"status":"payed","goods":[{"id":1,"count":1,"naming":""}]}'
    sleep 0.1
    delete_order
    sleep 0.1
}

echo -e "${ORANGE}TEST: test_create_get_delete_order$NC"
test_create_get_delete_order
echo -e "${ORANGE}TEST: test_create_update_get_delete_order$NC"
test_create_update_get_delete_order
echo -e "${ORANGE}TEST: test_billing$NC"
test_billing
echo -e "${ORANGE}TEST: test_update_after_billing$NC"
test_update_after_billing
