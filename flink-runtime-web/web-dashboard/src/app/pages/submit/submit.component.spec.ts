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

import { HttpEventType } from '@angular/common/http';
import { ChangeDetectorRef } from '@angular/core';
import { UntypedFormBuilder } from '@angular/forms';
import { Router } from '@angular/router';
import { of, throwError } from 'rxjs';

import { JarFilesItem } from '@flink-runtime-web/interfaces';
import { JarService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SubmitComponent } from './submit.component';

const cdr = { markForCheck: vi.fn(), detectChanges: vi.fn() } as unknown as ChangeDetectorRef;

describe('SubmitComponent', () => {
  const loadJarList = vi.fn();
  const uploadJar = vi.fn();
  const deleteJar = vi.fn();
  const runJob = vi.fn();
  const forceRefresh = vi.fn();
  const navigate = vi.fn().mockResolvedValue(true);
  let component: SubmitComponent;

  beforeEach(() => {
    loadJarList.mockReset().mockReturnValue(of({ files: [], address: 'http://jm', error: undefined }));
    uploadJar.mockReset();
    deleteJar.mockReset().mockReturnValue(of(undefined));
    runJob.mockReset();
    forceRefresh.mockReset();
    navigate.mockClear();
    component = new SubmitComponent(
      { loadJarList, uploadJar, deleteJar, runJob } as unknown as JarService,
      { refresh$: of(true), forceRefresh } as unknown as StatusService,
      new UntypedFormBuilder(),
      { navigate } as unknown as Router,
      cdr
    );
  });

  it('loads the uploaded jars on init', () => {
    loadJarList.mockReturnValue(of({ files: [{ id: 'jar-1' }], address: 'http://jm', error: undefined }));

    component.ngOnInit();

    expect(component.listOfJar).toEqual([{ id: 'jar-1' }]);
    expect(component.address).toBe('http://jm');
    expect(component.isLoading).toBe(false);
    expect(component.noAccess).toBe(false);
    expect(component.isYarn).toBe(false);
  });

  it('marks no-access when the jar list carries an error', () => {
    loadJarList.mockReturnValue(of({ files: [], address: '', error: 'forbidden' }));

    component.ngOnInit();

    expect(component.noAccess).toBe(true);
  });

  it('marks no-access when the jar list request fails', () => {
    loadJarList.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.noAccess).toBe(true);
    expect(component.isLoading).toBe(false);
  });

  it('expands a jar and prefills the entry class from its first entry', () => {
    component.ngOnInit();

    component.expandJar({ id: 'jar-1', entry: [{ name: 'com.example.Main' }] } as JarFilesItem);

    expect(component.expandedMap.get('jar-1')).toBe(true);
    expect(component.validateForm.get('entryClass')!.value).toBe('com.example.Main');

    // Expanding the same jar again collapses it.
    component.expandJar({ id: 'jar-1', entry: [{ name: 'com.example.Main' }] } as JarFilesItem);
    expect(component.expandedMap.get('jar-1')).toBe(false);
  });

  it('refreshes and collapses after deleting a jar', () => {
    component.ngOnInit();
    component.expandedMap.set('jar-1', true);

    component.deleteJar({ id: 'jar-1' } as JarFilesItem);

    expect(deleteJar).toHaveBeenCalledWith('jar-1');
    expect(forceRefresh).toHaveBeenCalled();
    expect(component.expandedMap.get('jar-1')).toBe(false);
  });

  it('tracks upload progress from http events', () => {
    uploadJar.mockReturnValue(of({ type: HttpEventType.UploadProgress, loaded: 50, total: 100 }));

    component.uploadJar(new File([''], 'app.jar'));

    expect(component.isUploading).toBe(true);
    expect(component.progress).toBe(50);
  });

  it('navigates to the new job after submitting', () => {
    component.ngOnInit();
    runJob.mockReturnValue(of({ jobid: 'job-9' }));

    component.submitJob({ id: 'jar-1' } as JarFilesItem);

    expect(navigate).toHaveBeenCalledWith(['job', 'running', 'job-9']);
  });
});
