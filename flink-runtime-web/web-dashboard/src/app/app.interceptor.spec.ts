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

import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpHeaders,
  HttpRequest,
  HttpResponse
} from '@angular/common/http';
import { Observable, of, throwError } from 'rxjs';

import { StatusService } from '@flink-runtime-web/services';
import { NzNotificationService } from 'ng-zorro-antd/notification';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AppInterceptor } from './app.interceptor';

describe('AppInterceptor', () => {
  let interceptor: AppInterceptor;
  let statusService: StatusService;
  let notificationService: NzNotificationService;
  let handle: ReturnType<typeof vi.fn<(req: HttpRequest<unknown>) => Observable<HttpEvent<unknown>>>>;
  let handler: HttpHandler;
  let originalLocation: Location;

  beforeEach(() => {
    statusService = { listOfErrorMessage: [] } as unknown as StatusService;
    notificationService = { info: vi.fn() } as unknown as NzNotificationService;
    handle = vi.fn<(req: HttpRequest<unknown>) => Observable<HttpEvent<unknown>>>();
    handler = { handle };
    interceptor = new AppInterceptor(statusService, notificationService);

    originalLocation = window.location;
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { ...originalLocation, href: 'https://dashboard.example/jobs' }
    });
  });

  afterEach(() => {
    Object.defineProperty(window, 'location', { configurable: true, value: originalLocation });
  });

  it('clones the outgoing request to include credentials', () => {
    handle.mockReturnValue(of(new HttpResponse({ status: 200 })));
    const request = new HttpRequest('GET', '/overview');

    interceptor.intercept(request, handler).subscribe();

    expect(handle).toHaveBeenCalledWith(expect.objectContaining({ withCredentials: true }));
  });

  it('passes through a successful response unchanged', () => {
    const response = new HttpResponse({ status: 200 });
    handle.mockReturnValue(of(response));
    const request = new HttpRequest('GET', '/overview');

    let emitted: unknown;
    interceptor.intercept(request, handler).subscribe(value => (emitted = value));

    expect(emitted).toBe(response);
  });

  it('navigates to the Location header on a redirect response', () => {
    const redirect = new HttpErrorResponse({
      status: 307,
      url: '/jobs/123',
      headers: new HttpHeaders({ Location: 'https://dashboard.example/login' })
    });
    handle.mockReturnValue(throwError(() => redirect));

    interceptor.intercept(new HttpRequest('GET', '/jobs/123'), handler).subscribe({ error: () => {} });

    expect(window.location.href).toBe('https://dashboard.example/login');
  });

  it('does not navigate on a redirect status without a Location header', () => {
    const redirect = new HttpErrorResponse({ status: 301, url: '/jobs/123' });
    handle.mockReturnValue(throwError(() => redirect));

    interceptor.intercept(new HttpRequest('GET', '/jobs/123'), handler).subscribe({ error: () => {} });

    expect(window.location.href).toBe('https://dashboard.example/jobs');
  });

  it('re-throws the original error after handling it', () => {
    const error = new HttpErrorResponse({ status: 500, url: '/jobs/123' });
    handle.mockReturnValue(throwError(() => error));

    let caught: unknown;
    interceptor.intercept(new HttpRequest('GET', '/jobs/123'), handler).subscribe({ error: err => (caught = err) });

    expect(caught).toBe(error);
  });

  it('surfaces a server error message via notification and the status service cache', () => {
    const error = new HttpErrorResponse({
      status: 500,
      url: '/jobs/123/exceptions',
      error: { errors: ['Something failed at line 10'] }
    });
    handle.mockReturnValue(throwError(() => error));

    interceptor.intercept(new HttpRequest('GET', '/jobs/123/exceptions'), handler).subscribe({ error: () => {} });

    expect(statusService.listOfErrorMessage).toEqual(['Something failed at line 10']);
    expect(notificationService.info).toHaveBeenCalledWith(
      'Server Response Message:',
      'Something failed\n at line 10',
      expect.objectContaining({ nzDuration: 0 })
    );
  });

  it.each(['/jobs/123/checkpoints', '/jobs/123/checkpoints/config'])(
    'suppresses the notification for the ignored URL %s',
    url => {
      const error = new HttpErrorResponse({ status: 500, url, error: { errors: ['Some error'] } });
      handle.mockReturnValue(throwError(() => error));

      interceptor.intercept(new HttpRequest('GET', url), handler).subscribe({ error: () => {} });

      expect(statusService.listOfErrorMessage).toEqual([]);
      expect(notificationService.info).not.toHaveBeenCalled();
    }
  );

  it.each(['File not found.', 'Resource not found.'])('suppresses the notification for the message "%s"', message => {
    const error = new HttpErrorResponse({ status: 404, url: '/jobs/123/exceptions', error: { errors: [message] } });
    handle.mockReturnValue(throwError(() => error));

    interceptor.intercept(new HttpRequest('GET', '/jobs/123/exceptions'), handler).subscribe({ error: () => {} });

    expect(statusService.listOfErrorMessage).toEqual([]);
    expect(notificationService.info).not.toHaveBeenCalled();
  });
});
