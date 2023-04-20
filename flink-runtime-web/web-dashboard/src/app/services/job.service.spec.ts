/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { JobResourceRequirements } from '@flink-runtime-web/interfaces/job-resource-requirements';
import { ConfigService } from '@flink-runtime-web/services/config.service';
import { JobService } from '@flink-runtime-web/services/job.service';

const clone = function (jobResourceRequirements: JobResourceRequirements): JobResourceRequirements {
  return JSON.parse(JSON.stringify(jobResourceRequirements));
};

describe('Job Service', () => {
  let configService: ConfigService;
  let jobService: JobService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{ provide: ConfigService, useValue: new ConfigService() }]
    });

    configService = TestBed.inject(ConfigService);
    jobService = TestBed.inject(JobService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  it('#changeDesiredParallelism', done => {
    const jobId = 'apache-flink';
    const jobResourceRequirements: JobResourceRequirements = {
      firstVertex: {
        parallelism: {
          lowerBound: 1,
          upperBound: 1
        }
      },
      secondVertex: {
        parallelism: {
          lowerBound: 1,
          upperBound: 2
        }
      },
      thirdVertex: {
        parallelism: {
          lowerBound: 1,
          upperBound: 3
        }
      }
    };
    const desiredParallelism = new Map<string, number>();
    desiredParallelism.set('firstVertex', 3);
    desiredParallelism.set('secondVertex', 2);
    desiredParallelism.set('thirdVertex', 1);
    jobService.changeDesiredParallelism(jobId, desiredParallelism).subscribe(() => {
      expect(true).toBe(true);
      done();
    });

    const firstRequest = httpTestingController.expectOne({
      method: 'GET',
      url: `${configService.BASE_URL}/jobs/${jobId}/resource-requirements`
    });
    firstRequest.flush(clone(jobResourceRequirements));

    const expected = clone(jobResourceRequirements);
    for (const k of desiredParallelism.keys()) {
      const newUpperBound = desiredParallelism.get(k);
      if (newUpperBound != undefined) {
        expected[k].parallelism.upperBound = newUpperBound;
      }
    }

    const secondRequest = httpTestingController.expectOne(request => {
      let matches = true;
      matches = matches && request.method == 'PUT';
      matches = matches && request.url == `${configService.BASE_URL}/jobs/${jobId}/resource-requirements`;
      if (matches) {
        expect(request.body).toEqual(expected);
      }
      return true;
    });
    secondRequest.flush(expected);
  });
});
