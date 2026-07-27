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

import { HttpErrorResponse, HttpHandler, HttpHeaders, HttpRequest, HttpResponse } from '@angular/common/http';
import { of, Subject, throwError } from 'rxjs';

import { StatusService } from '@flink-runtime-web/services';
import { NzNotificationService } from 'ng-zorro-antd/notification';
import { type Mock, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AppInterceptor } from './app.interceptor';

describe('AppInterceptor', () => {
  let interceptor: AppInterceptor;
  let statusService: StatusService;
  let notificationService: NzNotificationService;
  let handle: Mock<HttpHandler['handle']>;
  let handler: HttpHandler;
  let originalLocation: Location;
  let warningClose: Subject<boolean>;

  beforeEach(() => {
    statusService = {
      listOfErrorMessage: [],
      networkFailureCount: 0,
      networkFailureThreshold: 5,
      networkErrorNotificationId: null,
      markAppForCheck: vi.fn()
    } as unknown as StatusService;
    warningClose = new Subject<boolean>();
    notificationService = {
      info: vi.fn(),
      warning: vi.fn().mockReturnValue({ messageId: 'net-err-1', onClose: warningClose }),
      remove: vi.fn()
    } as unknown as NzNotificationService;
    handle = vi.fn();
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

    let caught: unknown;
    interceptor
      .intercept(new HttpRequest('GET', '/jobs/123/exceptions'), handler)
      .subscribe({ error: err => (caught = err) });

    expect(caught).toBe(error);
    expect(statusService.listOfErrorMessage).toEqual(['Something failed at line 10']);
    expect(notificationService.info).toHaveBeenCalledWith(
      'Server Response Message:',
      'Something failed\n at line 10',
      expect.objectContaining({ nzDuration: 0 })
    );
    expect(statusService.markAppForCheck).toHaveBeenCalled();
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

  it.each([0, 500, 503])(
    'counts a bodyless status %i as a network failure without surfacing a warning below the threshold',
    status => {
      const error = new HttpErrorResponse({ status, url: '/overview' });
      handle.mockReturnValue(throwError(() => error));

      interceptor.intercept(new HttpRequest('GET', '/overview'), handler).subscribe({ error: () => {} });

      expect(statusService.networkFailureCount).toBe(1);
      expect(statusService.networkErrorNotificationId).toBeNull();
      expect(notificationService.warning).not.toHaveBeenCalled();
    }
  );

  it('surfaces the network-error warning once the failure threshold is reached, and only once', () => {
    statusService.networkFailureCount = statusService.networkFailureThreshold - 1;
    handle.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 500, url: '/overview' })));

    interceptor.intercept(new HttpRequest('GET', '/overview'), handler).subscribe({ error: () => {} });

    expect(statusService.networkFailureCount).toBe(statusService.networkFailureThreshold);
    expect(notificationService.warning).toHaveBeenCalledWith(
      'Network Error:',
      'Connection lost or server error.',
      expect.objectContaining({ nzDuration: 0 })
    );
    expect(statusService.networkErrorNotificationId).toBe('net-err-1');

    // A further failure while a notification is already visible must not open a second one.
    interceptor.intercept(new HttpRequest('GET', '/overview'), handler).subscribe({ error: () => {} });

    expect(statusService.networkFailureCount).toBe(statusService.networkFailureThreshold + 1);
    expect(notificationService.warning).toHaveBeenCalledTimes(1);
  });

  it('clears the network-error state when the warning notification closes', () => {
    statusService.networkFailureCount = statusService.networkFailureThreshold - 1;
    handle.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 500, url: '/overview' })));

    interceptor.intercept(new HttpRequest('GET', '/overview'), handler).subscribe({ error: () => {} });
    expect(statusService.networkErrorNotificationId).toBe('net-err-1');

    warningClose.next(true);

    expect(statusService.networkErrorNotificationId).toBeNull();
    expect(statusService.networkFailureCount).toBe(0);
  });

  it('resets the failure count and removes the notification on a successful response', () => {
    statusService.networkFailureCount = 3;
    statusService.networkErrorNotificationId = 'net-err-1';
    handle.mockReturnValue(of(new HttpResponse({ status: 200 })));

    interceptor.intercept(new HttpRequest('GET', '/overview'), handler).subscribe();

    expect(statusService.networkFailureCount).toBe(0);
    expect(notificationService.remove).toHaveBeenCalledWith('net-err-1');
  });
});
