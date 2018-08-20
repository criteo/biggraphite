#!/usr/bin/env python
# Copyright 2018 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Asynchronous task runner."""

import enum
from concurrent import futures
from datetime import datetime

from biggraphite.cli.web import context
from biggraphite.cli.web.capture import Capture


# TODO: add a scheduler to purge old tasks
class TaskRunner:
    """Task runner."""

    def __init__(self):
        """Init TaskRunner."""
        self.tasks = []
        self._executor = futures.ThreadPoolExecutor(
            max_workers=10, thread_name_prefix="bgutil_worker"
        )

    def submit(self, label, command, opts):
        """Submit a bgutil command to run it asynchronously."""
        task = BgUtilTask(label, datetime.now())
        self.tasks.append(task)

        def _done_callback(f):
            try:
                result = f.result()
                task.completed(result)
            except futures.CancelledError as e:
                task.cancelled(e)
            except futures.TimeoutError as e:
                task.timed_out(e)
            except Exception as e:
                task.failed(e)

        future = self._executor.submit(
            self._wrap_command, task, context.accessor, command, opts
        )
        task.submitted()
        future.add_done_callback(_done_callback)

    @staticmethod
    def _wrap_command(task, accessor, cmd, opts):
        task.started()
        with Capture() as capture:
            cmd.run(accessor, opts)
        return capture.get_content()


class BgUtilTask:
    """BgUtil task."""

    def __init__(
        self, label, submitted_on, started_on=None, completed_on=None, result=None
    ):
        """Init a bgutil task."""
        self.label = label
        self.submitted_on = submitted_on
        self.started_on = started_on
        self.completed_on = completed_on
        self.result = result
        self.status = BgUtilTaskStatus.CREATED

    def submitted(self):
        """Mark the task as submitted."""
        self.submitted_on = datetime.now()
        self.status = BgUtilTaskStatus.SUBMITTED

    def started(self):
        """Mark the task as started."""
        self.started_on = datetime.now()
        self.status = BgUtilTaskStatus.STARTED

    def completed(self, result):
        """Mark the task as completed."""
        self.result = result
        self.completed_on = datetime.now()
        self.status = BgUtilTaskStatus.COMPLETED

    def failed(self, result=None):
        """Mark the task as failed."""
        self.result = result
        self.status = BgUtilTaskStatus.FAILED

    def cancelled(self, result=None):
        """Mark the task as cancelled."""
        self.result = result
        self.status = BgUtilTaskStatus.CANCELLED

    def timed_out(self, result=None):
        """Mark the task as timed out."""
        self.result = result
        self.status = BgUtilTaskStatus.TIMED_OUT


class BgUtilTaskStatus(enum.Enum):
    """Status of a BgUtilTask."""

    CREATED = "created"
    SUBMITTED = "submitted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    FAILED = "failed"
