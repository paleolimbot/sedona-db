# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

destination <- file.path(R_PACKAGE_DIR, paste0("libs", R_ARCH))
dir.create(destination, recursive = TRUE, showWarnings = FALSE)

package_dll <- Sys.glob(paste0("*", SHLIB_EXT))
native_dlls <- Sys.glob(file.path("vcpkg-dlls", "*.dll"))
dlls <- c(package_dll, native_dlls)

if (length(dlls) == 0L || !all(file.copy(dlls, destination, overwrite = TRUE))) {
  stop("Failed to install sedonadb native libraries")
}
