const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const serviceGet = require('../../../lib/api/serviceGet');

const authInfo = makeAuthInfo('accessKey1');
const log = new DummyRequestLogger();
const namespace = 'default';
const bucketName1 = 'bucketname1';
const bucketName2 = 'bucketname2';
const bucketName3 = 'bucketname3';

describe('serviceGet API', () => {
    beforeEach(() => {
        cleanup();
    });

    const serviceGetRequest = {
        parsedHost: 's3.amazonaws.com',
        headers: { host: 's3.amazonaws.com' },
        url: '/',
    };

    test('should return the list of buckets owned by the user', done => {
        const testbucketPutRequest1 = {
            namespace,
            bucketName: bucketName1,
            url: '/',
            headers: { host: `${bucketName1}.s3.amazonaws.com` },
        };
        const testbucketPutRequest2 = {
            namespace,
            bucketName: bucketName2,
            url: '/',
            headers: { host: `${bucketName2}.s3.amazonaws.com` },
        };
        const testbucketPutRequest3 = {
            namespace,
            bucketName: bucketName3,
            url: '/',
            headers: { host: `${bucketName3}.s3.amazonaws.com` },
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testbucketPutRequest1, log, next);
            },
            function waterfall2(corsHeaders, next) {
                bucketPut(authInfo, testbucketPutRequest2, log, next);
            },
            function waterfall3(corsHeaders, next) {
                bucketPut(authInfo, testbucketPutRequest3, log, next);
            },
            function waterfall4(corsHeaders, next) {
                serviceGet(authInfo, serviceGetRequest, log, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            },
        ], (err, result) => {
            expect(result.ListAllMyBucketsResult
                .Buckets[0].Bucket.length).toBe(3);
            expect(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[0].Name[0]).toBe(bucketName1);
            expect(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[1].Name[0]).toBe(bucketName2);
            expect(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[2].Name[0]).toBe(bucketName3);
            expect(result.ListAllMyBucketsResult.$.xmlns).toBe('http://s3.amazonaws.com/doc/2006-03-01/');
            done();
        });
    });

    test('should prevent anonymous user from accessing getService API', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        serviceGet(publicAuthInfo, serviceGetRequest, log, err => {
            assert.deepStrictEqual(err, errors.AccessDenied);
            done();
        });
    });
});
